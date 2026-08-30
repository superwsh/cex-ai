package com.cex.matching.infrastructure.wal;

import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Pattern;

/** WAL 根目录与交易对目录的统一安全策略。 */
final class WalPathPolicy {
    private static final Pattern CANONICAL_SYMBOL = Pattern.compile("[A-Z0-9_-]+");

    /** 禁止实例化路径策略工具类。 */
    private WalPathPolicy() {
    }

    /**
     * 将 WAL 根目录转换为绝对规范路径。
     *
     * @param root WAL 根目录
     * @return 绝对且已规范化的根目录
     */
    static Path normalizeRoot(Path root) {
        return Objects.requireNonNull(root, "WAL 根目录不能为空").toAbsolutePath().normalize();
    }

    /**
     * 校验交易对并确保其目录直接位于 WAL 根目录下。
     *
     * @param root 已规范化的 WAL 根目录
     * @param symbol 待校验的交易对
     * @return 已校验的规范交易对
     */
    static String validateSymbol(Path root, String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        if (symbol.equals(".") || symbol.equals("..") || symbol.contains("/") || symbol.contains("\\")) {
            throw new IllegalArgumentException("交易对不能包含路径分隔符或相对路径");
        }
        if (!CANONICAL_SYMBOL.matcher(symbol).matches()) {
            throw new IllegalArgumentException("交易对必须使用大写字母、数字、下划线或连字符");
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
        Path symbolDirectory = root.resolve(symbolPath).normalize();
        if (!Objects.equals(symbolDirectory.getParent(), root)) {
            throw new IllegalArgumentException("交易对目录必须位于 WAL 根目录下");
        }
        return symbol;
    }
}
