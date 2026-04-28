package dev.bublik.cli.cassandra.postgresql;

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
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static dev.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CassandraToPostgresTest {
    private static int rows = 10_000;
    private static boolean sync = false;
    private static CassandraContainer source = new CassandraContainer("cassandra")
            .withEnv("CASSANDRA_USER", "cassandra")
            .withEnv("CASSANDRA_PASSWORD", "cassandra")
            .withEnv("CASSANDRA_USER_DEFINED_FUNCTIONS_ENABLED", "true")
            .withEnv("CASSANDRA_AUTHENTICATOR", "PasswordAuthenticator")
            .withEnv("CASSANDRA_NUM_TOKENS", "16")
            .withCopyToContainer(MountableFile.forClasspathResource("./cassandra/cassandra/conf/docker-entrypoint.sh"), "/usr/local/bin/docker-entrypoint.sh")
            .withInitScript("./cassandra/postgresql/sql/cs-init.cql")
            .withExposedPorts(9042);
    private static JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./cassandra/postgresql/sql/pg-init.sql");

    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(java.util.Collections.singletonList("9042:9042"));
        source.start();
        target.setPortBindings(java.util.Collections.singletonList("5432:5432"));
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
    public void primitiveTypes() throws InterruptedException, IOException {
        Properties sourceProperties = getJdbcPropertiesOfCassandra(source);
        Properties targetProperties = getJdbcProperties(target);
        boolean result = getResult(
                "./cassandra/postgresql/yaml/cs2pg.yaml",
                "./cassandra/postgresql/json/cs2pg.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);
//        Thread.sleep(190_000);
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
        properties.setProperty("user", "cassandra");
        properties.setProperty("password", "cassandra");
        properties.setProperty("datacenter", "datacenter1");
        properties.setProperty("batchSize", "256");
        return properties;
    }
}
