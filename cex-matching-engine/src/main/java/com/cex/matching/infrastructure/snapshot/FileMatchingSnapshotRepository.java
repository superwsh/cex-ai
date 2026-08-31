package com.cex.matching.infrastructure.snapshot;

import com.cex.matching.application.port.outbound.MatchingSnapshotRepository;
import com.cex.matching.domain.model.OrderBookSnapshot;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Base64;
import java.util.Optional;

/** 使用临时文件替换保证写入完整性的订单簿快照仓库。 */
@Component
public class FileMatchingSnapshotRepository implements MatchingSnapshotRepository {

    private final ObjectMapper objectMapper;
    private final Path snapshotDirectory;

    /**
     * 创建文件快照仓库并确保目录可用。
     *
     * @param objectMapper JSON 序列化工具
     * @param snapshotDirectoryPath 快照文件目录
     */
    public FileMatchingSnapshotRepository(ObjectMapper objectMapper,
                                          @Value("${cex.matching.snapshot-directory:data/matching/snapshot}") String snapshotDirectoryPath) {
        this.objectMapper = objectMapper;
        this.snapshotDirectory = Path.of(snapshotDirectoryPath);
        try {
            Files.createDirectories(snapshotDirectory);
        } catch (IOException exception) {
            throw new IllegalStateException("无法创建撮合快照目录: " + snapshotDirectory, exception);
        }
    }

    /**
     * 将完整快照先写入临时文件再替换正式文件。
     *
     * @param snapshot 已在命令边界生成的订单簿快照
     */
    @Override
    public void save(OrderBookSnapshot snapshot) {
        Path directory = directoryOf(snapshot.symbol());
        Path target = directory.resolve("snapshot-" + snapshot.sequence() + ".bin");
        Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        try {
            Files.createDirectories(directory);
            byte[] content = objectMapper.writeValueAsBytes(snapshot);
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(content));
                channel.force(true);
            }
            moveAtomically(temporary, target);
        } catch (IOException exception) {
            throw new IllegalStateException("撮合快照写入失败: symbol=" + snapshot.symbol(), exception);
        }
    }

    /**
     * 读取指定交易对最近一次完整快照。
     *
     * @param symbol 需要恢复的交易对
     * @return 快照不存在时为空
     */
    @Override
    public Optional<OrderBookSnapshot> load(String symbol) {
        Path directory = directoryOf(symbol);
        if (!Files.isDirectory(directory)) {
            return Optional.empty();
        }
        try (var files = Files.list(directory)) {
            for (Path file : files.filter(this::isCompletedSnapshot)
                    .sorted(Comparator.comparingLong(this::sequenceOf).reversed()).toList()) {
                try {
                    OrderBookSnapshot snapshot = objectMapper.readValue(Files.readAllBytes(file), OrderBookSnapshot.class);
                    if (symbol.equals(snapshot.symbol()) && snapshot.sequence() == sequenceOf(file)) {
                        return Optional.of(snapshot);
                    }
                } catch (IOException | RuntimeException ignoredCorruptSnapshot) {
                    // 尝试更早的完整快照，.tmp 与损坏版本均不会阻断恢复。
                }
            }
            return Optional.empty();
        } catch (IOException exception) {
            throw new IllegalStateException("撮合快照读取失败: symbol=" + symbol, exception);
        }
    }

    /**
     * 以原子替换提交完整快照，文件系统不支持时退化为覆盖移动。
     *
     * @param temporary 临时快照文件
     * @param target 正式快照文件
     */
    private void moveAtomically(Path temporary, Path target) throws IOException {
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException exception) {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * 将交易对编码为安全文件名。
     *
     * @param symbol 交易对
     * @return 交易对对应快照文件
     */
    private Path directoryOf(String symbol) {
        String fileName = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(symbol.getBytes(StandardCharsets.UTF_8));
        return snapshotDirectory.resolve(fileName);
    }

    private boolean isCompletedSnapshot(Path path) {
        String name = path.getFileName().toString();
        return name.matches("snapshot-\\d+\\.bin");
    }

    private long sequenceOf(Path path) {
        String name = path.getFileName().toString();
        return Long.parseLong(name.substring("snapshot-".length(), name.length() - ".bin".length()));
    }
}
