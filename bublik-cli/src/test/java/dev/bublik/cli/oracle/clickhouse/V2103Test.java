package dev.bublik.cli.oracle.clickhouse;

import dev.bublik.cli.TestResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;

import static dev.bublik.cli.TestUtils.*;

public class V2103Test {
    private static int rows = 20000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("./oracle/clickhouse/sql/oracle/01_init.sql");

    private static final DockerImageName CLICKHOUSE_21_3 = DockerImageName
            .parse("yandex/clickhouse-server:21.3")
            .asCompatibleSubstituteFor("clickhouse/clickhouse-server");

    private static ClickHouseContainer target = new ClickHouseContainer(CLICKHOUSE_21_3)
            .withClasspathResourceMapping(
                    "./oracle/clickhouse/sql/allTypes.sql",
                    "/docker-entrypoint-initdb.d/allTypes.sql",
                    BindMode.READ_ONLY);

    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(java.util.Collections.singletonList("1521:1521"));
        source.start();
        target.setPortBindings(java.util.List.of("8123:8123", "9000:9000"));
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
    void allTypes() throws IOException, InterruptedException {
        TestResult result = getResultCount(
                "./oracle/clickhouse/yaml/ora2click.yaml",
                "./oracle/clickhouse/json/ora2click.json",
                rows,
                sync,
                getJdbcProperties(source),
                getStaticJdbcProperties());
//        Thread.sleep(200_000);
//        assertEquals(result.sourceCount(), result.targetCount());
    }

    public Properties getStaticJdbcProperties() {
        Properties properties = new Properties();
        properties.setProperty("url", "jdbc:clickhouse:http://localhost:8123/default");
        properties.setProperty("database", "default");
        properties.setProperty("user", "test");
        properties.setProperty("password", "test");
        return properties;
    }
}
