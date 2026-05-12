package dev.bublik.cli.oracle.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.cli.TestUtils;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;

import static dev.bublik.cli.TestUtils.getJdbcProperties;
import static dev.bublik.cli.TestUtils.getResultCount;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OracleToPostgresTest {
    private static int rows = 20000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("./oracle/postgres/sql/oracle/01_init.sql");
    private static JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./oracle/postgres/sql/pg-init-empty.sql");

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
        while (source.isRunning() || target.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void columnOrder() throws IOException, InterruptedException {
        TestResult result = getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/columnOrder.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
//        Thread.sleep(100_000);
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    void parted() throws IOException, InterruptedException {
        TestResult result = TestUtils.getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/parted.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.targetCount(), result.sourceCount());
    }

    @Test
    void leftJoin() throws IOException, InterruptedException {
        TestResult result = TestUtils.getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/leftJoin.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.targetCount(), result.sourceCount());
    }

    @Test
    void columnFromMany() throws IOException {
        TestResult result = TestUtils.getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/columnFromMany.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.targetCount(), result.sourceCount());
    }

    @Test
    void interval() throws IOException, InterruptedException {
        TestResult result = TestUtils.getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/interval.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.targetCount(), result.sourceCount());
    }
}
