package com.cex.matching.infrastructure.wal;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class FileWalReader implements WalReader {
    private static final Pattern WAL_FILE_NAME = Pattern.compile("wal-(\\d{6})\\.log");

    private final Path root;
    private final WalCodec codec;

    public FileWalReader(Path root, WalCodec codec) {
        this.root = Objects.requireNonNull(root, "WAL 根目录不能为空");
        this.codec = Objects.requireNonNull(codec, "WAL 编解码器不能为空");
    }

    @Override
    public WalReadResult read(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        Path directory = root.resolve(symbol);
        if (Files.notExists(directory)) {
            return new WalReadResult(List.of(), false);
        }

        List<Path> files = listWalFiles(directory);
        List<WalRecord> records = new ArrayList<>();
        boolean incompleteTail = false;
        for (int index = 0; index < files.size(); index++) {
            WalReadResult fileResult = readFile(files.get(index));
            if (fileResult.incompleteTail() && index != files.size() - 1) {
                throw new WalCorruptionException("非最后 WAL 文件包含不完整尾部记录");
            }
            records.addAll(fileResult.records());
            incompleteTail = fileResult.incompleteTail();
        }
        verifyIncreasingSequences(records);
        return new WalReadResult(records, incompleteTail);
    }

    public WalReadResult readFile(Path file) {
        Objects.requireNonNull(file, "WAL 文件路径不能为空");
        try {
            return decodeFile(Files.readAllBytes(file));
        } catch (IOException e) {
            throw new WalException("读取 WAL 文件失败", e);
        }
    }

    private List<Path> listWalFiles(Path directory) {
        try (Stream<Path> paths = Files.list(directory)) {
            return paths.filter(Files::isRegularFile)
                    .filter(path -> WAL_FILE_NAME.matcher(path.getFileName().toString()).matches())
                    .sorted(Comparator.comparingInt(this::fileNumber))
                    .toList();
        } catch (IOException e) {
            throw new WalException("扫描 WAL 目录失败", e);
        }
    }

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

    private String line(byte[] content, int start, int end) {
        int length = end - start;
        if (length > 0 && content[end - 1] == '\r') {
            length--;
        }
        return new String(content, start, length, StandardCharsets.UTF_8);
    }

    private int fileNumber(Path file) {
        String fileName = file.getFileName().toString();
        return Integer.parseInt(WAL_FILE_NAME.matcher(fileName).replaceFirst("$1"));
    }

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
