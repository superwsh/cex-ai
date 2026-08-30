package com.cex.matching.infrastructure.wal;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.AnnotatedElementContext;
import org.junit.jupiter.api.io.TempDirFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** 在项目 target 下创建可执行真实路径校验的 WAL 测试目录。 */
public final class WalTestTempDirFactory implements TempDirFactory {

    /**
     * 创建由 JUnit 生命周期负责清理的 WAL 临时目录。
     *
     * @param elementContext 临时目录注入元素上下文
     * @param extensionContext JUnit 扩展上下文
     * @return 新建的 WAL 测试临时目录
     * @throws IOException 创建目录失败时抛出
     */
    @Override
    public Path createTempDirectory(AnnotatedElementContext elementContext,
                                    ExtensionContext extensionContext) throws IOException {
        Path parent = Path.of(System.getProperty("user.dir"), "target", "wal-test-temp");
        Files.createDirectories(parent);
        return Files.createTempDirectory(parent, "junit-");
    }
}
