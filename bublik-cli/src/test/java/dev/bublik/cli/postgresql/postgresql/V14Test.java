package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.cli.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.SQLException;

import static dev.bublik.cli.TestUtils.getJdbcProperties;
import static dev.bublik.cli.TestUtils.getResultCount;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class V14Test {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres:14-alpine")
            .withDatabaseName("postgres")
//            .withCopyFileToContainer(MountableFile.forHostPath("images/bublik.png"), "/var/lib/postgresql/bublik.png")
            .withInitScript("postgresql/postgresql/sql/pg-init-v14.sql");
    private static JdbcDatabaseContainer<?> target = source;

    @BeforeAll
    static void setUp() throws SQLException {
        MountableFile mf = MountableFile.forClasspathResource("./images/bublik.png");
        source.addFileSystemBind(mf.getResolvedPath(), "/var/lib/postgresql/bublik.png", BindMode.READ_ONLY);
        source.setPortBindings(java.util.Collections.singletonList("5432:5432"));
        source.start();
    }

    @AfterAll
    static void clear() {
        source.stop();
        while (source.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void allTypes() throws IOException {
        TestResult result = TestUtils.getResultCount(
                "postgresql/postgresql/yaml/pg2pg.yaml",
                "postgresql/postgresql/json/allTypes-v14.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        assertEquals(result.targetCount(), result.sourceCount());
    }
}
