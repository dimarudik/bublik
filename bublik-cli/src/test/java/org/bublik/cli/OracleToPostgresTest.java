package org.bublik.cli;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;

import static org.bublik.cli.TestUtils.getResult;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class OracleToPostgresTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("./ora2pg/sql/00_init.sql");
    private static JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres:latest")
            .withDatabaseName("postgres")
//            .withCopyFileToContainer(MountableFile.forHostPath("images/bublik.png"), "/var/lib/postgresql/bublik.png")
            .withInitScript("./ora2pg/sql/pg-init-empty.sql");

    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(java.util.Collections.singletonList("1521:1521"));
        source.start();
        MountableFile mf = MountableFile.forClasspathResource("./images/bublik.png");
        target.addFileSystemBind(mf.getResolvedPath(), "/var/lib/postgresql/bublik.png", BindMode.READ_ONLY);
        target.setPortBindings(java.util.Collections.singletonList("5432:5432"));
        target.start();
    }

    @AfterAll
    static void clear() {
        source.stop();
        target.stop();
    }

    @Test
    void parted() throws IOException {
        TestResult result = getResult(
                "ora2pg/ora2pg.yaml",
                "ora2pg/cases/parted.json",
                rows,
                sync,
                source,
                target);
        assertEquals(result.targetCount(), result.sourceCount());
    }

    @Test
    void leftJoin() throws IOException {
        TestResult result = getResult(
                "ora2pg/ora2pg.yaml",
                "ora2pg/cases/leftJoin.json",
                rows,
                sync,
                source,
                target);
        assertEquals(result.targetCount(), result.sourceCount());
    }

    @Test
    void columnFromMany() throws IOException {
        TestResult result = getResult(
                "ora2pg/ora2pg.yaml",
                "ora2pg/cases/columnFromMany.json",
                rows,
                sync,
                source,
                target);
        assertEquals(result.targetCount(), result.sourceCount());
    }

    @Test
    void interval() throws IOException {
        TestResult result = getResult(
                "ora2pg/ora2pg.yaml",
                "ora2pg/cases/interval.json",
                rows,
                sync,
                source,
                target);
        assertEquals(result.targetCount(), result.sourceCount());
    }
}
