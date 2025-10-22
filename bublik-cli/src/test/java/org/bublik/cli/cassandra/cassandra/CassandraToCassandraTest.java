package org.bublik.cli.cassandra.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.bublik.cli.App;
import org.bublik.cli.TestResult;
import org.bublik.cli.TestUtils;
import org.bublik.cli.addons.Utils;
import org.bublik.core.model.Config;
import org.bublik.core.model.ConnectionProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Properties;

import static org.bublik.cli.App.getConfigs;

public class CassandraToCassandraTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static CassandraContainer source = new CassandraContainer("cassandra")
            .withInitScript("./cassandra/cassandra/sql/cs-init.cql");
    private static CassandraContainer target = new CassandraContainer("cassandra")
            .withInitScript("./cassandra/cassandra/sql/cs-init-empty.cql");

    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(java.util.Collections.singletonList("9042:9042"));
        source.start();
        target.setPortBindings(java.util.Collections.singletonList("9043:9042"));
        target.start();
        while (!source.isRunning() && !target.isRunning()) {
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
    public void cassandraToCassandra() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("9042");
        Properties targetProperties = getPropertiesOfCassandra("9043");
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);
//        Thread.sleep(30_000);
//        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
//        assertEquals(result.sourceCount(), result.targetCount());
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

//        String fromQuery = "SELECT count(1) * 2 FROM public.likes l left join users u on u.id = l.user_id left join items i on i.id = l.item_id";
//        Long sourceCount = countRows(sourceProperties, fromQuery);
//        Long targetCount = countCassandra();
        return new TestResult(0, 0);
    }

    private Properties getPropertiesOfCassandra(String port) {
        Properties properties = new Properties();
        properties.setProperty("class", "org.bublik.cassandra.storage.CSPoolStorage");
        properties.setProperty("keyspace", "test");
        properties.setProperty("user", "test");
        properties.setProperty("password", "test");
        properties.setProperty("datacenter", "datacenter1");
        properties.setProperty("port", port);
        properties.setProperty("batchSize", "256");
        return properties;
    }
}
