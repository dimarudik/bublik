package org.bublik.cli.oracle.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.bublik.cli.App;
import org.bublik.cli.TestResult;
import org.bublik.cli.TestUtils;
import org.bublik.cli.addons.Utils;
import org.bublik.core.model.Config;
import org.bublik.core.model.ConnectionProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.oracle.OracleContainer;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.bublik.cli.App.getConfigs;
import static org.bublik.cli.TestUtils.getJdbcProperties;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OracleToCassandraTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("./oracle/cassandra/sql/oracle/ora-init.sql");
    private static CassandraContainer target = new CassandraContainer("cassandra")
            .withInitScript("./oracle/cassandra/sql/cs-init.cql");

    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(java.util.Collections.singletonList("1521:1521"));
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
    public void oracleToCassandra() throws InterruptedException, IOException {
        Properties sourceProperties = getJdbcProperties(source);
        Properties targetProperties = getJdbcPropertiesOfCassandra(target);
        TestResult result = getResult(
                "./oracle/cassandra/yaml/ora2cs.yaml",
                "./oracle/cassandra/json/ora2cs.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    public static TestResult getResult(String connectionPropertyFile,
                                       String mappingFile,
                                       int rows,
                                       boolean sync,
                                       Properties sourceProperties,
                                       Properties targetProperties) throws IOException {
        return getResult(connectionPropertyFile, mappingFile, rows, sync, sourceProperties, targetProperties, null);
    }

    public static TestResult getResult(String connectionPropertyFile,
                                       String mappingFile,
                                       int rows,
                                       boolean sync,
                                       Properties sourceProperties,
                                       Properties targetProperties,
                                       String chunkTableName) throws IOException {
        ConnectionProperty cp = Utils.connectionProperty(TestUtils.getFilePath(connectionPropertyFile));
        List<Config> configs = getConfigs(TestUtils.getFilePath(mappingFile));

        App.runProcess(cp, configs, rows, sync, chunkTableName);

        String fromQuery = "SELECT count(1) * 2 FROM test.likes l join users u on u.id = l.user_id join items i on i.id = l.item_id";
        Long sourceCount = countRows(sourceProperties, fromQuery);
        Long targetCount = countCassandra();
        return new TestResult(sourceCount, targetCount);
    }

    private static Long countCassandra() {
        CqlSession cqlSession = CqlSession
                .builder()
                .addContactPoint(target.getContactPoint())
                .withLocalDatacenter(target.getLocalDatacenter())
                .build();
        com.datastax.oss.driver.api.core.cql.ResultSet resultSet = cqlSession.execute("SELECT user_id FROM test.user");
        long rowCount = 0;
        for (com.datastax.oss.driver.api.core.cql.Row row : resultSet) {
            rowCount++;
        }
        com.datastax.oss.driver.api.core.cql.ResultSet resultSet1 = cqlSession.execute("SELECT item_id FROM test.item");
        for (com.datastax.oss.driver.api.core.cql.Row row : resultSet1) {
            rowCount++;
        }
        cqlSession.close();
        return rowCount;
    }

    public static Long countRows(Properties p, String query) {
        try (Connection connection =
                     DriverManager.getConnection(p.getProperty("url"), p.getProperty("user"), p.getProperty("password"))) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            resultSet.next();
            return resultSet.getLong(1);
        }
        catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    private Properties getJdbcPropertiesOfCassandra(CassandraContainer target) {
        Properties properties = new Properties();
        properties.setProperty("class", "org.bublik.cassandra.storage.CSPoolStorage");
        properties.setProperty("keyspace", "test");
        properties.setProperty("user", "test");
        properties.setProperty("password", "test");
        properties.setProperty("datacenter", "datacenter1");
        properties.setProperty("port", "9042");
        properties.setProperty("batchSize", "256");
        return properties;
    }
}

