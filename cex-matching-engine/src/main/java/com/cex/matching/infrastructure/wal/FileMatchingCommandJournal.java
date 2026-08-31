package com.cex.matching.infrastructure.wal;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.matching.application.service.MatchingCommandJournal;
import com.cex.matching.application.service.RecordedMatchingCommand;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** 使用每交易对独立文件实现的命令预写日志。 */
@Component
public class FileMatchingCommandJournal implements MatchingCommandJournal {

    private final ObjectMapper objectMapper;
    private final Path walDirectory;

    /**
     * 创建文件 WAL 并确保日志目录可用。
     *
     * @param objectMapper JSON 序列化工具
     * @param walDirectoryPath WAL 文件目录
     */
    public FileMatchingCommandJournal(ObjectMapper objectMapper,
                                      @Value("${cex.matching.wal-directory:data/matching/wal}") String walDirectoryPath) {
        this.objectMapper = objectMapper;
        this.walDirectory = Path.of(walDirectoryPath);
        try {
            Files.createDirectories(walDirectory);
        } catch (IOException exception) {
            throw new IllegalStateException("无法创建撮合 WAL 目录: " + walDirectory, exception);
        }
    }

    /**
     * 在执行撮合前追加并强制刷盘，保证宕机后可重放。
     *
     * @param symbol 交易对
     * @param sequence 该交易对内的严格递增序号
     * @param event 待执行的订单事件
     */
    @Override
    public void append(String symbol, long sequence, OrderEvent event) {
        String line = serialize(new RecordedMatchingCommand(symbol, sequence, event)) + System.lineSeparator();
        try (FileChannel channel = FileChannel.open(fileOf(symbol), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            channel.write(ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8)));
            channel.force(true);
        } catch (IOException exception) {
            throw new IllegalStateException("撮合 WAL 写入失败: symbol=" + symbol + ", sequence=" + sequence, exception);
        }
    }

    /**
     * 读取指定快照序号之后的命令记录。
     *
     * @param symbol 交易对
     * @param sequence 已持久化快照的序号
     * @return 序号大于给定值的命令记录
     */
    @Override
    public List<RecordedMatchingCommand> readAfter(String symbol, long sequence) {
        Path file = fileOf(symbol);
        if (!Files.exists(file)) {
            return List.of();
        }
        try {
            return Files.readAllLines(file, StandardCharsets.UTF_8).stream().filter(line -> !line.isBlank())
                    .map(this::deserialize).filter(command -> command.sequence() > sequence).toList();
        } catch (IOException exception) {
            throw new IllegalStateException("撮合 WAL 读取失败: symbol=" + symbol, exception);
        }
    }

    /**
     * 扫描全部存在 WAL 的交易对。
     *
     * @return 交易对集合
     */
    @Override
    public Set<String> symbols() {
        try (var files = Files.list(walDirectory)) {
            return files.filter(path -> path.getFileName().toString().endsWith(".wal"))
                    .map(this::symbolOf).collect(Collectors.toUnmodifiableSet());
        } catch (IOException exception) {
            throw new IllegalStateException("撮合 WAL 目录扫描失败: " + walDirectory, exception);
        }
    }

    /**
     * 在快照成功落盘后删除不再需要重放的旧记录。
     *
     * @param symbol 交易对
     * @param inclusiveSequence 可安全删除的最大序号（含）
     */
    @Override
    public void compact(String symbol, long inclusiveSequence) {
        Path target = fileOf(symbol);
        if (!Files.exists(target)) {
            return;
        }
        Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        try {
            String retained = Files.readAllLines(target, StandardCharsets.UTF_8).stream().filter(line -> !line.isBlank())
                    .map(this::deserialize).filter(command -> command.sequence() > inclusiveSequence)
                    .map(this::serialize).collect(Collectors.joining(System.lineSeparator()));
            Files.writeString(temporary, retained.isBlank() ? "" : retained + System.lineSeparator(), StandardCharsets.UTF_8);
            moveReplacingTarget(temporary, target);
        } catch (IOException exception) {
            throw new IllegalStateException("撮合 WAL 裁剪失败: symbol=" + symbol, exception);
        }
    }

    /**
     * 优先以原子替换完成 WAL 切换；文件系统不支持原子移动时退化为同目录替换。
     *
     * @param temporary 已完整写入的临时文件
     * @param target 正式 WAL 文件
     * @throws IOException 文件移动失败时抛出
     */
    private void moveReplacingTarget(Path temporary, Path target) throws IOException {
        try {
            Files.move(temporary, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException exception) {
            Files.move(temporary, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * 将命令转换为单行 JSON，以便按追加顺序恢复。
     *
     * @param command 待写入的日志命令
     * @return 单行 JSON
     */
    private String serialize(RecordedMatchingCommand command) {
        try {
            return objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("撮合 WAL 序列化失败", exception);
        }
    }

    /**
     * 解析 WAL 中的一行 JSON。
     *
     * @param line WAL 原始行
     * @return 已校验的命令记录
     */
    private RecordedMatchingCommand deserialize(String line) {
        try {
            return objectMapper.readValue(line, RecordedMatchingCommand.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("撮合 WAL 内容损坏", exception);
        }
    }

    /**
     * 根据交易对得到稳定且不含路径字符的 WAL 文件路径。
     *
     * @param symbol 交易对
     * @return 对应 WAL 文件路径
     */
    private Path fileOf(String symbol) {
        String fileName = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(symbol.getBytes(StandardCharsets.UTF_8));
        return walDirectory.resolve(fileName + ".wal");
    }

    /**
     * 从 WAL 文件名解码交易对。
     *
     * @param file WAL 文件路径
     * @return 文件归属交易对
     */
    private String symbolOf(Path file) {
        String fileName = file.getFileName().toString();
        String encodedSymbol = fileName.substring(0, fileName.length() - ".wal".length());
        return new String(Base64.getUrlDecoder().decode(encodedSymbol), StandardCharsets.UTF_8);
    }
}
