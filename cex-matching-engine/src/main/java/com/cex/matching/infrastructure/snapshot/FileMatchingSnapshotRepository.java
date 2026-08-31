package com.cex.matching.infrastructure.snapshot;

import com.cex.matching.application.service.MatchingSnapshotRepository;
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
        Path target = fileOf(snapshot.symbol());
        Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        try {
            Files.writeString(temporary, objectMapper.writeValueAsString(snapshot), StandardCharsets.UTF_8);
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
        Path file = fileOf(symbol);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(Files.readString(file, StandardCharsets.UTF_8),
                    OrderBookSnapshot.class));
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
    private Path fileOf(String symbol) {
        String fileName = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(symbol.getBytes(StandardCharsets.UTF_8));
        return snapshotDirectory.resolve(fileName + ".snapshot");
    }
}
