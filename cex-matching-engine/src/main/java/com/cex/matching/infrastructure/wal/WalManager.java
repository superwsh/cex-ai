package com.cex.matching.infrastructure.wal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
        this.root = Objects.requireNonNull(root, "WAL 根目录不能为空").normalize();
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
        return new ActiveWriter(fileNumber, writerFactory.open(walPath(symbol, fileNumber), codec));
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
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        if (symbol.equals(".") || symbol.equals("..") || symbol.contains("/") || symbol.contains("\\")) {
            throw new IllegalArgumentException("交易对不能包含路径分隔符或相对路径");
        }
        Path symbolPath;
        try {
            symbolPath = Path.of(symbol);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("交易对目录名不合法", e);
        }
        if (symbolPath.isAbsolute()) {
            throw new IllegalArgumentException("交易对不能使用绝对路径");
        }
        Path symbolDirectory = root.resolve(symbol).normalize();
        if (!Objects.equals(symbolDirectory.getParent(), root)) {
            throw new IllegalArgumentException("交易对目录必须位于 WAL 根目录下");
        }
        return symbol;
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
