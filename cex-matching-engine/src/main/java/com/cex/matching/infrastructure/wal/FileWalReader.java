package com.cex.matching.infrastructure.wal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** 从本地文件读取并校验 WAL 记录。 */
public final class FileWalReader implements WalReader {
    private static final Pattern WAL_FILE_NAME = Pattern.compile("wal-(\\d{6})\\.log");

    private final Path root;
    private final WalCodec codec;

    /**
     * 创建基于安全根目录的 WAL 读取器。
     *
     * @param root WAL 根目录
     * @param codec WAL 编解码器
     */
    public FileWalReader(Path root, WalCodec codec) {
        this.root = WalPathPolicy.normalizeRoot(root);
        this.codec = Objects.requireNonNull(codec, "WAL 编解码器不能为空");
    }

    /**
     * 按文件编号读取指定规范交易对的全部 WAL 记录。
     *
     * @param symbol 规范交易对
     * @return 合并后的 WAL 读取结果
     */
    @Override
    public WalReadResult read(String symbol) {
        Path directory = WalPathPolicy.resolveSymbolDirectory(root, symbol, false);
        if (Files.notExists(directory)) {
            return new WalReadResult(List.of(), false);
        }

        List<Path> files = listWalFiles(directory);
        List<WalRecord> records = new ArrayList<>();
        boolean incompleteTail = false;
        for (int index = 0; index < files.size(); index++) {
            WalReadResult fileResult = readFile(files.get(index));
            if (fileResult.incompleteTail() && hasLaterPhysicalRecord(files, index)) {
                throw new WalCorruptionException("非最后 WAL 文件包含不完整尾部记录");
            }
            records.addAll(fileResult.records());
            incompleteTail |= fileResult.incompleteTail();
        }
        verifyIncreasingSequences(records);
        return new WalReadResult(records, incompleteTail);
    }

    /**
     * 读取并校验单个 WAL 文件。
     *
     * @param file WAL 文件路径
     * @return 文件内有效记录与尾部状态
     */
    public WalReadResult readFile(Path file) {
        Objects.requireNonNull(file, "WAL 文件路径不能为空");
        try {
            return decodeFile(Files.readAllBytes(file));
        } catch (IOException e) {
            throw new WalException("读取 WAL 文件失败", e);
        }
    }

    /**
     * 列出并按编号排序交易对目录内的合法 WAL 文件。
     *
     * @param directory 交易对目录
     * @return 不可变的 WAL 文件列表
     */
    private List<Path> listWalFiles(Path directory) {
        try (Stream<Path> paths = Files.list(directory)) {
            List<Path> files = new ArrayList<>();
            paths.forEach(path -> addWalFile(path, files));
            files.sort(Comparator.comparingInt(this::fileNumber));
            return List.copyOf(files);
        } catch (IOException e) {
            throw new WalException("扫描 WAL 目录失败", e);
        } catch (UncheckedIOException e) {
            throw new WalException("扫描 WAL 目录失败", e.getCause());
        }
    }

    /**
     * 将满足文件名与常规文件条件的路径加入结果。
     *
     * @param path 待检查路径
     * @param files 收集结果
     */
    private void addWalFile(Path path, List<Path> files) {
        try {
            BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
            if (attributes.isRegularFile() && WAL_FILE_NAME.matcher(path.getFileName().toString()).matches()) {
                files.add(path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 判断当前文件后是否仍有非空物理 WAL 文件。
     *
     * @param files 已排序 WAL 文件
     * @param currentIndex 当前文件下标
     * @return 后续存在非空文件时返回 true
     */
    private boolean hasLaterPhysicalRecord(List<Path> files, int currentIndex) {
        for (int index = currentIndex + 1; index < files.size(); index++) {
            try {
                if (Files.size(files.get(index)) > 0L) {
                    return true;
                }
            } catch (IOException e) {
                throw new WalException("读取 WAL 文件元数据失败", e);
            }
        }
        return false;
    }

    /**
     * 按物理换行边界解码文件内容并识别可忽略尾部。
     *
     * @param content WAL 文件完整字节
     * @return 有效记录与尾部状态
     */
    private WalReadResult decodeFile(byte[] content) {
        List<WalRecord> records = new ArrayList<>();
        int start = 0;
        for (int index = 0; index < content.length; index++) {
            if (content[index] == '\n') {
                String line = line(content, start, index);
                boolean lastTerminatedRecord = index == content.length - 1;
                try {
                    records.add(codec.decode(line));
                } catch (WalCorruptionException e) {
                    if (lastTerminatedRecord) {
                        return new WalReadResult(records, true);
                    }
                    throw e;
                }
                start = index + 1;
            }
        }
        if (start < content.length) {
            try {
                codec.decode(line(content, start, content.length));
            } catch (WalCorruptionException ignored) {
                // 最后一条未换行记录允许在故障恢复时被忽略。
            }
            return new WalReadResult(records, true);
        }
        return new WalReadResult(records, false);
    }

    /**
     * 将指定字节区间转换为 UTF-8 行并兼容 CRLF。
     *
     * @param content WAL 文件字节
     * @param start 行起始下标
     * @param end 行结束下标
     * @return 不含换行符的文本行
     */
    private String line(byte[] content, int start, int end) {
        int length = end - start;
        if (length > 0 && content[end - 1] == '\r') {
            length--;
        }
        return new String(content, start, length, StandardCharsets.UTF_8);
    }

    /**
     * 从合法 WAL 文件名提取文件编号。
     *
     * @param file WAL 文件路径
     * @return 六位十进制文件编号
     */
    private int fileNumber(Path file) {
        String fileName = file.getFileName().toString();
        return Integer.parseInt(WAL_FILE_NAME.matcher(fileName).replaceFirst("$1"));
    }

    /**
     * 校验合并后的记录序列严格递增。
     *
     * @param records 合并后的 WAL 记录
     */
    private void verifyIncreasingSequences(List<WalRecord> records) {
        long previous = 0L;
        boolean hasPrevious = false;
        for (WalRecord record : records) {
            if (hasPrevious && record.sequence() <= previous) {
                throw new WalCorruptionException("WAL sequence 必须严格递增");
            }
            previous = record.sequence();
            hasPrevious = true;
        }
    }
}
