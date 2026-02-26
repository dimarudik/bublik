package dev.bublik.cli.postgresql.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import dev.bublik.cli.App;
import dev.bublik.cli.TestUtils;
import dev.bublik.cli.addons.Utils;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static dev.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.assertTrue;

//@Disabled
public class ToMapTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./postgresql/cassandra/sql/pg-to-map.sql");
    private static CassandraContainer target = new CassandraContainer("cassandra")
            .withInitScript("./postgresql/cassandra/sql/cs-to-list-all.cql");

    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(java.util.Collections.singletonList("5432:5432"));
        source.start();
        target.setPortBindings(java.util.Collections.singletonList("9042:9042"));
        target.start();
        while (!source.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
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
    public void toMap() throws InterruptedException, IOException {
        Properties sourceProperties = getJdbcProperties(source);
        Properties targetProperties = getJdbcPropertiesOfCassandra(target);
        boolean result = getResult(
                "./postgresql/cassandra/yaml/pg2cs-to-list.yaml",
                "./postgresql/cassandra/json/to-map.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);
        assertTrue(result);
    }

    public static boolean getResult(String connectionPropertyFile,
                                    String mappingFile,
                                    int rows,
                                    boolean sync,
                                    Properties sourceProperties,
                                    Properties targetProperties) throws IOException {
        ConnectionProperty cp = Utils.connectionProperty(TestUtils.getFilePath(connectionPropertyFile));
        List<Config> configs = getConfigs(TestUtils.getFilePath(mappingFile));

        App.runProcess(cp, configs, rows, sync);

        return resultCassandra();
    }

    private static boolean resultCassandra() {
        int pageSize = 10000;
        CqlSession cqlSession = CqlSession
                .builder()
                .addContactPoint(target.getContactPoint())
                .withLocalDatacenter(target.getLocalDatacenter())
                .build();
        SimpleStatement stmt1 = SimpleStatement.builder("SELECT * FROM test.to_list")
                .setPageSize(pageSize) // set page size
                .build();
        com.datastax.oss.driver.api.core.cql.ResultSet resultSet = cqlSession.execute(stmt1);
        for (com.datastax.oss.driver.api.core.cql.Row row : resultSet) {
            if (!(row.getInt("id") == 1)) {
                return false;
            }
            Map<Integer, String> kv1 = row.getMap("kv1", Integer.class, String.class);
            System.out.println(kv1);
            if(!kv1.get(1).equals("2025-01-01 00:00:00")) {
                return false;
            }
            if(!kv1.get(2).equals("user1")) {
                return false;
            }
            Map<String, String> kv2 = row.getMap("kv2", String.class, String.class);
            System.out.println(kv2);
            if(!kv2.get("user1").equals("user1@gmail.comv1")) {
                return false;
            }
        }
        cqlSession.close();
        return true;
    }

    private static Properties getJdbcProperties(JdbcDatabaseContainer<?> db) {
        Properties properties = new Properties();
        properties.setProperty("url", db.getJdbcUrl());
        properties.setProperty("user", db.getUsername());
        properties.setProperty("password", db.getPassword());
        return properties;
    }

    private Properties getJdbcPropertiesOfCassandra(CassandraContainer target) {
        Properties properties = new Properties();
        properties.setProperty("class", "org.bublik.cassandra.storage.CSPoolStorage");
        properties.setProperty("keyspace", "test");
        properties.setProperty("user", "test");
        properties.setProperty("password", "test");
        properties.setProperty("datacenter", "datacenter1");
        properties.setProperty("batchSize", "256");
        return properties;
    }
}
