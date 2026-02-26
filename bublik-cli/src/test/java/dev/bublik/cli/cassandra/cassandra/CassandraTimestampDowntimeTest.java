package dev.bublik.cli.cassandra.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import dev.bublik.cassandra.storage.CSPool;
import dev.bublik.cli.App;
import dev.bublik.cli.TestResult;
import dev.bublik.cli.TestUtils;
import dev.bublik.cli.addons.Utils;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Predicate;

import static dev.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;

// +
//@Disabled
public class CassandraTimestampDowntimeTest {
    private static int rows = 50000;
    private static boolean sync = false;

    private static CassandraContainer source = new CassandraContainer("cassandra")
            .withExposedPorts(9042)
            .withEnv("CASSANDRA_USER_DEFINED_FUNCTIONS_ENABLED", "true")
            .withInitScript("./cassandra/cassandra/sql/cs-init-downtime.cql");
    private static CassandraContainer target = new CassandraContainer("cassandra")
            .withExposedPorts(9042)
            .withInitScript("./cassandra/cassandra/sql/cs-init-downtime-empty.cql");

    @BeforeAll
    static void setUp() throws SQLException, IOException, InterruptedException {
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
    public void withoutDowntimeBasedOnTimestamp() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        init(sourceProperties);
        init(targetProperties);
        long expectedTimestamp1 = getTimestamp(targetProperties);
        getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs12.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, v1 FROM ",
                null,
                null);
        long obtainedTimestamp1 = getTimestamp(targetProperties);
        System.out.println("1) expected timestamp: " + expectedTimestamp1 + ", obtained timestamp: " + obtainedTimestamp1);

        assertEquals(expectedTimestamp1, obtainedTimestamp1);

        init(sourceProperties);
        getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs12.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, v1 FROM ",
                null,
                null);
        long expectedTimestamp2 = getTimestamp(targetProperties);
        init(targetProperties);
        long obtainedTimestamp2 = getTimestamp(targetProperties);
        System.out.println("2) expected timestamp: " + expectedTimestamp2 + ", obtained timestamp: " + obtainedTimestamp2);
    }

    private Properties getPropertiesOfCassandra(String hosts) {
        Properties properties = new Properties();
        properties.setProperty("class", "org.bublik.cassandra.storage.CassandraStorage");
        properties.setProperty("keyspace", "test");
        properties.setProperty("hosts", hosts);
        properties.setProperty("user", "test");
        properties.setProperty("password", "test");
        properties.setProperty("datacenter", "datacenter1");
        properties.setProperty("batchSize", "256");
        return properties;
    }

    public static TestResult getResult(String connectionPropertyFile,
                                       String mappingFile,
                                       int rows,
                                       boolean sync,
                                       Properties sourceProperties,
                                       Properties targetProperties,
                                       String query,
                                       Predicate<Row> sourcePredicate,
                                       Predicate<Row> targetPredicate) throws IOException {
        ConnectionProperty cp = Utils.connectionProperty(TestUtils.getFilePath(connectionPropertyFile));
        List<Config> configs = getConfigs(TestUtils.getFilePath(mappingFile));

        App.runProcess(cp, configs, rows, sync, null);

        long sourceCount = 0;
        long targetCount = 0;
        for (Config config : configs) {
            sourceCount += countCassandra(sourceProperties, query,config.fromSchemaName() + "." + config.fromTableName(), sourcePredicate, config.fetchWhereClause());
            targetCount += countCassandra(targetProperties, query,
                    (config.toSchemaName() == null ? config.fromSchemaName() : config.toSchemaName()) + "." +
                            (config.toTableName() == null ? config.fromTableName() : config.toTableName()),
                    targetPredicate, null);
        }
        return new TestResult(sourceCount, targetCount);
    }

    private static long countCassandra(Properties properties, String query, String tableName, Predicate<Row> p, String where) {
        CSPool csPool = new CSPool(properties, 2);
        CqlSession cqlSession = csPool.getCqlSession();
        String q = query + tableName + ((where == null || where.isEmpty()) ? "" : " WHERE " + where);
        System.out.println(q);
        ResultSet resultSet = cqlSession.execute(q);
        long rowCount = 0;
        for (Row row : resultSet) {
            if (p == null) {
                rowCount++;
            } else if (p.test(row)) {
                rowCount++;
            }
        }
        csPool.closeCqlSession();
        return rowCount;
    }

    private void init(Properties properties) {
        CSPool csPool = new CSPool(properties, 2);
        CqlSession cqlSession = csPool.getCqlSession();
        String insert = "insert into test.t1 (id, v1) values (3, 3)";
        cqlSession.execute(insert);
        csPool.closeCqlSession();
    }

    private long getTimestamp(Properties properties) {
        CSPool csPool = new CSPool(properties, 2);
        CqlSession cqlSession = csPool.getCqlSession();
        String getTimestamp = "select writetime(v1) from test.t1 where id = 3";
        ResultSet resultSet = cqlSession.execute(getTimestamp);
        long timestamp = Objects.requireNonNull(resultSet.one()).getLong(0);
        csPool.closeCqlSession();
        return timestamp;
    }
}
