package dev.bublik.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.DummyTable;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.shaded.org.checkerframework.checker.nullness.qual.NonNull;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InitCassandraMigrationTest {
    static final CassandraContainer cassandraContainer = new CassandraContainer(
            DockerImageName.parse("cassandra"));

    static CqlSession initSession;

    @BeforeAll
    static void beforeAll() throws Exception {
        cassandraContainer.start();

        initSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraContainer.getHost(), cassandraContainer.getMappedPort(9042)))
                .withLocalDatacenter(cassandraContainer.getLocalDatacenter())
                .build();

        initSession.execute("CREATE KEYSPACE bublik_source WITH replication = " +
                "{'class': 'SimpleStrategy', 'replication_factor': 1};");
        initSession.execute("CREATE TABLE bublik_source.source_users (id int PRIMARY KEY, user_name text);");

        initSession.execute("INSERT INTO bublik_source.source_users (id, user_name) VALUES (1, 'Alice');");
        initSession.execute("INSERT INTO bublik_source.source_users (id, user_name) VALUES (2, 'Bob');");
        initSession.execute("INSERT INTO bublik_source.source_users (id, user_name) VALUES (3, 'Charlie');");

        initSession.execute("CREATE KEYSPACE bublik_target WITH replication = " +
                "{'class': 'SimpleStrategy', 'replication_factor': 1};");
        initSession.execute("CREATE TABLE bublik_target.target_users (id int PRIMARY KEY, user_name text);");
    }

    @AfterAll
    static void afterAll() {
        if (initSession != null) {
            initSession.close();
        }
        cassandraContainer.stop();
    }

    @Test
    void testCassandraToCassandraMigration() throws Exception {
        ConnectionProperty connectionProperty = getConnectionProperty();

        List<Config> configs = new ArrayList<>();
        Config config = Config.builder()
                .from("bublik_source", "source_users")
                .to("bublik_target", "target_users")
                .build();
        configs.add(config);

        Table chunkTable = new DummyTable.Builder("bublik_source", "bublik_chunks").build();

        StorageService.init(connectionProperty, configs, 1000, chunkTable);

        ResultSet rs = initSession.execute("SELECT id, user_name FROM bublik_target.target_users;");
        List<Row> rows = rs.all();

        assertEquals(3, rows.size(), "Метод init должен был перенести ровно 3 записи в Cassandra");

        boolean hasCharlie = rows.stream().anyMatch(row -> "Charlie".equals(row.getString("user_name")));
        assertTrue(hasCharlie, "Данные внутри строк целевой Cassandra должны полностью совпадать с источником");
    }

    private static @NonNull ConnectionProperty getConnectionProperty() {
        String cassandraHostStr = cassandraContainer.getHost() + ":" + cassandraContainer.getMappedPort(9042);

        Map<String, String> fromProps = new HashMap<>();
        fromProps.put("class", "dev.bublik.cassandra.storage.CassandraStorage");
        fromProps.put("hosts", cassandraHostStr);
        fromProps.put("keyspace", "bublik_source");
        fromProps.put("user", cassandraContainer.getUsername());
        fromProps.put("password", cassandraContainer.getPassword());
        fromProps.put("datacenter", cassandraContainer.getLocalDatacenter());
        fromProps.put("batchSize", "256");

        Map<String, String> toProps = new HashMap<>();
        toProps.put("class", "dev.bublik.cassandra.storage.CassandraStorage");
        toProps.put("hosts", cassandraHostStr);
        toProps.put("keyspace", "bublik_target");
        toProps.put("user", cassandraContainer.getUsername());
        toProps.put("password", cassandraContainer.getPassword());
        toProps.put("datacenter", cassandraContainer.getLocalDatacenter());
        toProps.put("batchSize", "256");

        return new ConnectionProperty(
                3,
                fromProps,
                toProps,
                new HashMap<>(),
                new HashMap<>()
        );
    }
}
