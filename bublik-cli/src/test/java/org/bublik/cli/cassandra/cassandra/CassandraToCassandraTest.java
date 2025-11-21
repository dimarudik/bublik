package org.bublik.cli.cassandra.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.bublik.cassandra.storage.CSPool;
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
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CassandraToCassandraTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static CassandraContainer source = new CassandraContainer("cassandra")
            .withInitScript("./cassandra/cassandra/sql/cs-init.cql");
    private static CassandraContainer target = new CassandraContainer("cassandra")
            .withInitScript("./cassandra/cassandra/sql/cs-init-empty.cql");

    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(Collections.singletonList("9042:9042"));
        source.start();
        target.setPortBindings(Collections.singletonList("9043:9042"));
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
    static void clear() throws InterruptedException {
        Thread.sleep(30_000);
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
// select id, uid, v1, v2, v3, v4, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4)  from test.t1;
    public void dataOnly() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("9042");
        Properties targetProperties = getPropertiesOfCassandra("9043");
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs1.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);
//        Thread.sleep(30_000);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
// select id, uid, v1, v2, v3, v4, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4)  from test.t1;
    public void withTtlOrTimestamp() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("9042");
        Properties targetProperties = getPropertiesOfCassandra("9043");
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs2.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);
//        Thread.sleep(80_000);
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

        long sourceCount = 0;
        long targetCount = 0;
        for (Config config : configs) {
            sourceCount += countCassandra(sourceProperties, config.fromSchemaName() + "." + config.fromTableName());
            targetCount += countCassandra(targetProperties, config.toSchemaName() + "." + config.toTableName());
        }
        return new TestResult(sourceCount, targetCount);
    }

    private static long countCassandra(Properties properties, String tableName) {
        CSPool csPool = new CSPool(properties, 2);
        CqlSession cqlSession = csPool.getCqlSession();
        ResultSet resultSet = cqlSession.execute("SELECT id FROM " + tableName);
        long rowCount = 0;
        for (Row row : resultSet) {
            rowCount++;
        }
        csPool.closeCqlSession();
        return rowCount;
    }

    private Properties getPropertiesOfCassandra(String port) {
        Properties properties = new Properties();
        properties.setProperty("class", "org.bublik.cassandra.storage.CassandraStorage");
        properties.setProperty("keyspace", "test");
        properties.setProperty("hosts", "localhost");
        properties.setProperty("user", "test");
        properties.setProperty("password", "test");
        properties.setProperty("datacenter", "datacenter1");
        properties.setProperty("port", port);
        properties.setProperty("batchSize", "256");
        return properties;
    }
}
