package dev.bublik.cassandra;

import com.clickhouse.client.api.Client;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import dev.bublik.cassandra.storage.CassandraStorage;
import dev.bublik.clickhouse.storage.ClickHouseStorage;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.DummyTable;
import dev.bublik.core.model.Table;
import dev.bublik.core.storage.Storage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static dev.bublik.cassandra.ContainerImageVersions.CLICKHOUSE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClickHouseMigrationTest {
    static final CassandraContainer cassandraContainer = new CassandraContainer(
            DockerImageName.parse("cassandra"));

    private static final DockerImageName CLICKHOUSE_STABLE = DockerImageName
            .parse(CLICKHOUSE)
            .asCompatibleSubstituteFor("clickhouse/clickhouse-server");

    static final ClickHouseContainer clickhouse = new ClickHouseContainer(CLICKHOUSE_STABLE);

    static CqlSession sourceSession;
    int batchSize = 256;
    String sourceKeyspace = "bublik_source";

    static Client clickhouseClient;

    @BeforeAll
    static void beforeAll() throws Exception {
        cassandraContainer.start();
        clickhouse.start();

        int expectedPoolSize = 3;
        DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, expectedPoolSize)
                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, expectedPoolSize)
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
                .build();

        InetSocketAddress contactPoint = new InetSocketAddress(
                cassandraContainer.getHost(),
                cassandraContainer.getMappedPort(9042)
        );

        sourceSession = CqlSession.builder()
                .addContactPoint(contactPoint)
                .withConfigLoader(configLoader)
                .withAuthCredentials(cassandraContainer.getUsername(), cassandraContainer.getPassword())
                .withLocalDatacenter(cassandraContainer.getLocalDatacenter())
                .build();

        sourceSession.execute("CREATE KEYSPACE bublik_source WITH replication = " +
                "{'class': 'SimpleStrategy', 'replication_factor': 1};");
        sourceSession.execute("CREATE TABLE bublik_source.source_users (id int PRIMARY KEY, user_name text);");

        sourceSession.execute("INSERT INTO bublik_source.source_users (id, user_name) VALUES (1, 'Alice');");
        sourceSession.execute("INSERT INTO bublik_source.source_users (id, user_name) VALUES (2, 'Bob');");
        sourceSession.execute("INSERT INTO bublik_source.source_users (id, user_name) VALUES (3, 'Charlie');");

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

        try (Connection conn = clickhouse.createConnection(""); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE default.target_users (id Int32, user_name String) ENGINE = MergeTree() ORDER BY id");
        }
    }

    @AfterAll
    static void afterAll() {
        if (sourceSession != null) sourceSession.close();
        if (clickhouseClient != null) clickhouseClient.close();
        cassandraContainer.stop();
        clickhouse.stop();
    }

    @Test
    @DisplayName("Сквозной тест миграции: Cassandra -> ClickHouse")
    void testCassandraToClickHouseMigration() throws Exception {
        Table sourceOutboxTable = new DummyTable.Builder(sourceKeyspace, "source_outbox")
                .build();
        Table targetOutboxTable = new DummyTable.Builder("default", "target_outbox")
                .build();

        Storage sourceStorage = new CassandraStorage.Builder(sourceSession, sourceKeyspace)
                .batchSize(batchSize)
                .outboxTable(sourceOutboxTable)
                .build();
        Storage targetStorage = new ClickHouseStorage.Builder(clickhouseClient)
                .outboxTable(targetOutboxTable)
                .build();

        assertEquals(5, targetStorage.getThreadCount(),
                "Количество потоков Бублика должно автоматически подстроиться под размер maxConnections нативного клиента ClickHouse");

        List<Config> configs = new java.util.ArrayList<>();
        Config config = Config.builder()
                .from(sourceKeyspace, "source_users")
                .to("default", "target_users")
                .build();
        configs.add(config);

        sourceStorage.start(targetStorage, configs, 1000);

        try (Connection conn = clickhouse.createConnection("");
             Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SELECT count(), min(user_name) FROM default.target_users")) {

            assertTrue(rs.next());
            long count = rs.getLong(1);
            String firstUser = rs.getString(2);

            assertEquals(3, count, "Количество строк в ClickHouse должно быть равно 3");
            assertEquals("Alice", firstUser, "Имена пользователей на стороне ClickHouse должны совпасть с Cassandra");
        }

        sourceStorage.closeStorage();
        targetStorage.closeStorage();
    }
}
