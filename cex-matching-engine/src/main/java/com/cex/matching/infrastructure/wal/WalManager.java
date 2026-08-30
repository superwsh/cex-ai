package com.cex.matching.infrastructure.wal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** 按交易对维护并滚动 WAL 文件的单线程管理器。 */
public final class WalManager implements AutoCloseable {
    private static final Pattern WAL_FILE_NAME = Pattern.compile("wal-(\\d{6})\\.log");
    private static final int MAX_FILE_NUMBER = 999_999;

    private final Path root;
    private final long maxFileSizeBytes;
    private final WalCodec codec;
    private final WriterFactory writerFactory;
    private final Map<String, ActiveWriter> activeWriters = new HashMap<>();
    private boolean closed;

    /**
     * 创建 WAL 管理器。
     *
     * @param root WAL 根目录
     * @param maxFileSizeBytes 单个 WAL 文件的最大字节数
     * @param codec WAL 编解码器
     */
    public WalManager(Path root, long maxFileSizeBytes, WalCodec codec) {
        this(root, maxFileSizeBytes, codec, FileWalWriter::new);
    }

    /**
     * 创建带有受控写入器工厂的 WAL 管理器，仅供同包测试验证资源关闭行为。
     *
     * @param root WAL 根目录
     * @param maxFileSizeBytes 单个 WAL 文件的最大字节数
     * @param codec WAL 编解码器
     * @param writerFactory WAL 写入器工厂
     */
    WalManager(Path root, long maxFileSizeBytes, WalCodec codec, WriterFactory writerFactory) {
        this.root = WalPathPolicy.normalizeRoot(root);
        if (maxFileSizeBytes <= 0L) {
            throw new IllegalArgumentException("WAL 文件大小阈值必须大于零");
        }
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.codec = Objects.requireNonNull(codec, "WAL 编解码器不能为空");
        this.writerFactory = Objects.requireNonNull(writerFactory, "WAL 写入器工厂不能为空");
    }

    /**
     * 追加一条 WAL 记录，必要时先将当前交易对的文件滚动到下一编号。
     *
     * @param record 要持久化的 WAL 记录
     */
    public void append(WalRecord record) {
        ensureOpen();
        WalRecord walRecord = Objects.requireNonNull(record, "WAL 记录不能为空");
        String symbol = validateSymbol(walRecord.symbol());
        long lineBytes = codec.encode(walRecord).getBytes(StandardCharsets.UTF_8).length + 1L;
        ActiveWriter activeWriter = activeWriters.computeIfAbsent(symbol, this::openActiveWriter);
        if (shouldRoll(activeWriter, lineBytes)) {
            activeWriter = roll(symbol, activeWriter);
        }
        activeWriter.writer().append(walRecord);
    }

    /**
     * 刷盘所有当前活动的 WAL 写入器。
     */
    public void flush() {
        ensureOpen();
        for (ActiveWriter activeWriter : activeWriters.values()) {
            activeWriter.writer().flush();
        }
    }

    /**
     * 关闭全部活动写入器；出现关闭异常时仍继续关闭其余写入器。
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        WalException firstFailure = null;
        for (ActiveWriter activeWriter : activeWriters.values()) {
            try {
                activeWriter.writer().close();
            } catch (WalException e) {
                firstFailure = appendFailure(firstFailure, e);
            }
        }
        activeWriters.clear();
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    /**
     * 判断完整新行是否会使非空文件超过配置阈值。
     *
     * @param activeWriter 当前活动写入器
     * @param lineBytes 完整编码行的 UTF-8 字节数
     * @return 超过阈值时返回 true
     */
    private boolean shouldRoll(ActiveWriter activeWriter, long lineBytes) {
        long writtenBytes = activeWriter.writer().writtenBytes();
        return writtenBytes > 0L && writtenBytes > maxFileSizeBytes - lineBytes;
    }

    /**
     * 关闭当前文件并打开同一交易对的下一编号文件。
     *
     * @param symbol 交易对
     * @param activeWriter 当前活动写入器
     * @return 新文件对应的活动写入器
     */
    private ActiveWriter roll(String symbol, ActiveWriter activeWriter) {
        if (activeWriter.fileNumber() == MAX_FILE_NUMBER) {
            throw new WalException("WAL 文件编号已达到 999999，无法继续滚动");
        }
        activeWriter.writer().close();
        ActiveWriter nextWriter = new ActiveWriter(activeWriter.fileNumber() + 1,
                writerFactory.open(walPath(symbol, activeWriter.fileNumber() + 1), codec));
        activeWriters.put(symbol, nextWriter);
        return nextWriter;
    }

    /**
     * 打开指定交易对已有最大编号文件，或创建首个编号文件。
     *
     * @param symbol 交易对
     * @return 可追加的活动写入器
     */
    private ActiveWriter openActiveWriter(String symbol) {
        int fileNumber = largestExistingFileNumber(root.resolve(symbol));
        Path path = walPath(symbol, fileNumber);
        repairRecoverableTail(path);
        return new ActiveWriter(fileNumber, writerFactory.open(path, codec));
    }

    /**
     * 校验已有最大编号文件，仅截断可恢复的最后一条损坏或未换行记录。
     *
     * @param path 待续写的 WAL 文件
     */
    private void repairRecoverableTail(Path path) {
        if (Files.notExists(path)) {
            return;
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long fileSize = channel.size();
            long lastValidBoundary = 0L;
            long currentPosition = 0L;
            ByteArrayOutputStream line = new ByteArrayOutputStream();
            ByteBuffer buffer = ByteBuffer.allocate(8 * 1024);
            while (channel.read(buffer) != -1) {
                buffer.flip();
                while (buffer.hasRemaining()) {
                    byte value = buffer.get();
                    currentPosition++;
                    if (value == '\n') {
                        if (!isValidLine(line.toByteArray())) {
                            if (currentPosition != fileSize) {
                                throw new WalCorruptionException("WAL 中间记录损坏，拒绝继续写入");
                            }
                            truncateAndForce(channel, lastValidBoundary);
                            return;
                        }
                        lastValidBoundary = currentPosition;
                        line.reset();
                    } else {
                        line.write(value);
                    }
                }
                buffer.clear();
            }
            if (line.size() > 0) {
                truncateAndForce(channel, lastValidBoundary);
            }
        } catch (WalCorruptionException e) {
            throw e;
        } catch (IOException e) {
            throw new WalException("校验或修复 WAL 文件尾部失败: " + path, e);
        }
    }

    /**
     * 校验一条以换行结束的 WAL 记录内容。
     *
     * @param bytes 不含换行符的记录字节
     * @return 记录 JSON 与校验和均有效时返回 true
     */
    private boolean isValidLine(byte[] bytes) {
        int length = bytes.length;
        if (length > 0 && bytes[length - 1] == '\r') {
            length--;
        }
        try {
            codec.decode(new String(bytes, 0, length, StandardCharsets.UTF_8));
            return true;
        } catch (WalCorruptionException e) {
            return false;
        }
    }

    /**
     * 将文件截断到最后有效记录边界并强制持久化元数据。
     *
     * @param channel 待截断的 WAL 文件通道
     * @param boundary 最后有效且以换行结束的字节边界
     * @throws IOException 截断或刷盘失败时抛出
     */
    private void truncateAndForce(FileChannel channel, long boundary) throws IOException {
        channel.truncate(boundary);
        channel.force(true);
    }

    /**
     * 扫描目录中合法 WAL 文件并返回最大编号，没有文件时返回首个编号。
     *
     * @param directory 交易对的 WAL 目录
     * @return 最大合法编号或 1
     */
    private int largestExistingFileNumber(Path directory) {
        if (Files.notExists(directory)) {
            return 1;
        }
        try (Stream<Path> paths = Files.list(directory)) {
            return paths.filter(Files::isRegularFile)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .map(WAL_FILE_NAME::matcher)
                    .filter(Matcher::matches)
                    .mapToInt(matcher -> Integer.parseInt(matcher.group(1)))
                    .filter(fileNumber -> fileNumber > 0)
                    .max()
                    .orElse(1);
        } catch (IOException e) {
            throw new WalException("扫描 WAL 目录失败", e);
        } catch (UncheckedIOException e) {
            throw new WalException("扫描 WAL 目录失败", e.getCause());
        }
    }

    /**
     * 生成交易对指定编号的 WAL 文件路径。
     *
     * @param symbol 交易对
     * @param fileNumber WAL 文件编号
     * @return WAL 文件完整路径
     */
    private Path walPath(String symbol, int fileNumber) {
        return root.resolve(symbol).resolve("wal-%06d.log".formatted(fileNumber));
    }

    /**
     * 校验交易对可作为 WAL 目录名使用。
     *
     * @param symbol 交易对
     * @return 已校验的交易对
     */
    private String validateSymbol(String symbol) {
        return WalPathPolicy.validateSymbol(root, symbol);
    }

    /**
     * 确保管理器未关闭。
     */
    private void ensureOpen() {
        if (closed) {
            throw new WalException("WAL 管理器已关闭");
        }
    }

    /**
     * 记录首个关闭异常，并将后续异常添加为抑制异常。
     *
     * @param firstFailure 已记录的首个异常，可为空
     * @param failure 当前关闭异常
     * @return 应最终抛出的首个异常
     */
    private WalException appendFailure(WalException firstFailure, WalException failure) {
        if (firstFailure == null) {
            return failure;
        }
        firstFailure.addSuppressed(failure);
        return firstFailure;
    }

    /** 活动 WAL 写入器及其文件编号。 */
    private record ActiveWriter(int fileNumber, WalWriter writer) {
    }

    /** WAL 写入器创建工厂，仅用于同包测试替换文件写入器。 */
    @FunctionalInterface
    interface WriterFactory {

        /**
         * 打开指定路径对应的 WAL 写入器。
         *
         * @param path WAL 文件路径
         * @param codec WAL 编解码器
         * @return 可用的 WAL 写入器
         */
        WalWriter open(Path path, WalCodec codec);
    }
}
