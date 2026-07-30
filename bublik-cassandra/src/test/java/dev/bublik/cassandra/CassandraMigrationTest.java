package dev.bublik.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import dev.bublik.cassandra.storage.CassandraStorage;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.PseudoTable;
import dev.bublik.core.model.Table;
import dev.bublik.core.storage.Storage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CassandraMigrationTest {
    static final CassandraContainer cassandraContainer = new CassandraContainer(
            DockerImageName.parse("cassandra"));

    static CqlSession sourceSession;
    static CqlSession targetSession;

    int batchSize = 256;
    String sourceKeyspace = "bublik_source";
    String targetKeyspace = "bublik_target";

    @BeforeAll
    static void beforeAll() throws Exception {
        cassandraContainer.start();

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

        targetSession = CqlSession.builder()
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

        targetSession.execute("CREATE KEYSPACE bublik_target WITH replication = " +
                "{'class': 'SimpleStrategy', 'replication_factor': 1};");
        targetSession.execute("CREATE TABLE bublik_target.target_users (id int PRIMARY KEY, user_name text);");
    }

    @AfterAll
    static void afterAll() {
        if (sourceSession != null) sourceSession.close();
        if (targetSession != null) targetSession.close();
        cassandraContainer.stop();
    }

    @Test
    void testOnlyTableNames() throws Exception {
        Table sourceOutboxTable = new PseudoTable(sourceKeyspace, "bublik");
        Table targetOutboxTable = new PseudoTable(targetKeyspace, "bublik");
        Storage sourceStorage = new CassandraStorage.Builder()
                .cqlSession(sourceSession)
                .batchSize(batchSize)
                .outboxTable(sourceOutboxTable)
                .build();
        Storage targetStorage = new CassandraStorage.Builder()
                .cqlSession(targetSession)
                .batchSize(batchSize)
                .outboxTable(targetOutboxTable)
                .build();

        assertEquals(3, sourceStorage.getThreadCount(),
                "Количество потоков Бублика должно автоматически подстроиться под размер пула CqlSession");

        List<Config> configs = new ArrayList<>();
        Config config = Config.builder()
                .from("bublik_source", "source_users")
                .to("bublik_target", "target_users")
                .build();
        configs.add(config);

        sourceStorage.start(targetStorage, configs, 1000);

        ResultSet rs = targetSession.execute("SELECT id, user_name FROM bublik_target.target_users;");
        List<Row> rows = rs.all();

        assertEquals(3, rows.size(), "В целевом Keyspace Cassandra должно быть ровно 3 записи");

        boolean hasCharlie = rows.stream().anyMatch(row -> "Charlie".equals(row.getString("user_name")));
        assertTrue(hasCharlie, "Данные внутри строк целевой Cassandra должны полностью совпадать с источником");

        sourceStorage.closeStorage();
        targetStorage.closeStorage();
    }
}
