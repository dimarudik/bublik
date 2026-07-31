package dev.bublik.postgres;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.bublik.cassandra.storage.CassandraStorage;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.DummyTable;
import dev.bublik.core.model.Table;
import dev.bublik.core.storage.Storage;
import dev.bublik.postgres.storage.PostgresStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CassandraMigrationTest {
    static final JdbcDatabaseContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres"));

    static final CassandraContainer cassandraContainer = new CassandraContainer(
            DockerImageName.parse("cassandra"));

    static HikariDataSource postgresDataSource;
    static CqlSession cassandraSession;
    int batchSize = 256;
    String outboxKeyspace = "bublik";

    @BeforeAll
    static void beforeAll() throws Exception {
        postgres.start();
        cassandraContainer.start();

        HikariConfig pgConfig = new HikariConfig();
        pgConfig.setJdbcUrl(postgres.getJdbcUrl());
        pgConfig.setUsername(postgres.getUsername());
        pgConfig.setPassword(postgres.getPassword());
        pgConfig.setMaximumPoolSize(5);
        postgresDataSource = new HikariDataSource(pgConfig);

        try (Connection conn = postgresDataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE source_users (id INT PRIMARY KEY, user_name VARCHAR(100))");
            stmt.execute("INSERT INTO source_users (id, user_name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
        }

        int expectedCassandraPoolSize = 3;
        DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, expectedCassandraPoolSize)
                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, expectedCassandraPoolSize)
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
                .build();

        cassandraSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraContainer.getHost(), cassandraContainer.getMappedPort(9042)))
                .withConfigLoader(configLoader)
                .withAuthCredentials(cassandraContainer.getUsername(), cassandraContainer.getPassword())
                .withLocalDatacenter(cassandraContainer.getLocalDatacenter())
                .build();

        cassandraSession.execute("CREATE KEYSPACE bublik WITH replication = " +
                "{'class': 'SimpleStrategy', 'replication_factor': 1};");

        cassandraSession.execute("CREATE TABLE bublik.target_users (id int PRIMARY KEY, user_name text);");
    }

    @AfterAll
    static void afterAll() {
        if (postgresDataSource != null) postgresDataSource.close();
        if (cassandraSession != null) cassandraSession.close();
        postgres.stop();
        cassandraContainer.stop();
    }

    @Test
    void testPostgresToCassandraMigration() throws Exception {
        Table sourceOutboxTable = new DummyTable("public", "bublik");
        Table targetOutboxTable = new DummyTable(outboxKeyspace, "bublik");
        Storage sourceStorage = new PostgresStorage.Builder(postgresDataSource)
                .outboxTable(sourceOutboxTable)
                .build();
        Storage targetStorage = new CassandraStorage.Builder(cassandraSession, outboxKeyspace)
                .batchSize(batchSize)
                .outboxTable(targetOutboxTable)
                .build();

        assertEquals(3, targetStorage.getThreadCount(),
                "Количество потоков Бублика должно автоматически подстроиться под CONNECTION_POOL_LOCAL_SIZE сессии Cassandra");

        List<Config> configs = new ArrayList<>();
        Config config = Config.builder()
                .from("public", "source_users")
                .to("bublik", "target_users")
                .build();
        configs.add(config);

        sourceStorage.start(targetStorage, configs, 1000);

        ResultSet rs = cassandraSession.execute("SELECT id, user_name FROM bublik.target_users;");
        List<Row> rows = rs.all();

        assertEquals(3, rows.size(), "Количество записей в Cassandra должно быть равно 3");

        boolean hasAlice = rows.stream().anyMatch(row -> "Alice".equals(row.getString("user_name")));
        assertTrue(hasAlice, "Данные внутри строк Cassandra должны совпадать с Postgres");

        sourceStorage.closeStorage();
        targetStorage.closeStorage();
    }
}
