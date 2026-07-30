package dev.bublik.postgres;

import com.clickhouse.client.api.Client;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.bublik.clickhouse.storage.ClickHouseStorage;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.PseudoTable;
import dev.bublik.core.model.Table;
import dev.bublik.core.storage.Storage;
import dev.bublik.postgres.storage.PostgresStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClickHouseMigrationTest {
    static final JdbcDatabaseContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres"));

    private static final DockerImageName CLICKHOUSE_LATEST = DockerImageName
            .parse("clickhouse")
            .asCompatibleSubstituteFor("clickhouse/clickhouse-server");
    static final ClickHouseContainer clickhouse = new ClickHouseContainer(CLICKHOUSE_LATEST);

    static HikariDataSource sourceDataSource;
    static Client clickhouseClient;

    @BeforeAll
    static void beforeAll() throws Exception {
        postgres.start();
        clickhouse.start();

        HikariConfig sourceConfig = new HikariConfig();
        sourceConfig.setJdbcUrl(postgres.getJdbcUrl());
        sourceConfig.setUsername(postgres.getUsername());
        sourceConfig.setPassword(postgres.getPassword());
        sourceConfig.setMaximumPoolSize(5);
        sourceDataSource = new HikariDataSource(sourceConfig);

        int expectedClickhousePoolSize = 5;
        String clickhouseHttpUrl = "http://" + clickhouse.getHost() + ":" + clickhouse.getMappedPort(8123);
        clickhouseClient = new Client.Builder()
                .addEndpoint(clickhouseHttpUrl)
                .setDefaultDatabase("default")
                .setUsername(clickhouse.getUsername())
                .setPassword(clickhouse.getPassword())
                .setMaxConnections(expectedClickhousePoolSize)
                .setConnectTimeout(10, ChronoUnit.SECONDS)
                .setSocketTimeout(5, ChronoUnit.MINUTES)
                .build();

        try (Connection conn = sourceDataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE source_users (id INT PRIMARY KEY, name VARCHAR(100))");
            stmt.execute("INSERT INTO source_users (id, name) VALUES (1, 'Alice')");
            stmt.execute("INSERT INTO source_users (id, name) VALUES (2, 'Bob')");
            stmt.execute("INSERT INTO source_users (id, name) VALUES (3, 'Charlie')");
        }

        try (Connection conn = clickhouse.createConnection(""); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE default.target_users (id Int32, name String) ENGINE = MergeTree() ORDER BY id");
        }
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (sourceDataSource != null) sourceDataSource.close();
        if (clickhouseClient != null) clickhouseClient.close();
        postgres.stop();
        clickhouse.stop();
    }

    @Test
    void testPostgresToClickHouseMigration() throws Exception {
        Table sourceOutboxTable = new PseudoTable("public", "source_outbox");
        Table targetOutboxTable = new PseudoTable("default", "target_outbox");

        Storage sourceStorage = new PostgresStorage.Builder()
                .dataSource(sourceDataSource)
                .outboxTable(sourceOutboxTable)
                .build();
        Storage targetStorage = new ClickHouseStorage(clickhouseClient, targetOutboxTable);

        assertEquals(5, targetStorage.getThreadCount(),
                "Количество потоков Бублика должно автоматически подстроиться под размер maxConnections нативного клиента ClickHouse");

        List<Config> configs = new ArrayList<>();
        Config config = Config.builder()
                .from("public", "source_users")
                .to("default", "target_users")
                .build();
        configs.add(config);

        sourceStorage.start(targetStorage, configs, 1000);

        try (Connection conn = clickhouse.createConnection("");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(), min(name) FROM default.target_users")) {

            assertTrue(rs.next());
            long count = rs.getLong(1);
            String firstUser = rs.getString(2);

            assertEquals(3, count, "Количество строк в ClickHouse должно быть равно 3");
            assertEquals("Alice", firstUser, "Имена пользователей на стороне ClickHouse должны совпасть с PostgreSQL");
        }

        sourceStorage.closeStorage();
        targetStorage.closeStorage();
    }
}
