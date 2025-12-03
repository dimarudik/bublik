package org.bublik.cli.cassandra.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
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
import java.util.function.Predicate;

import static org.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CassandraToCassandraTest {
    private static int rows = 50000;
    private static boolean sync = false;

    private static CassandraContainer source = new CassandraContainer("cassandra")
            .withExposedPorts(9042)
            .withConfigurationOverride("./cassandra/cassandra/conf")
            .withInitScript("./cassandra/cassandra/sql/cs-init.cql");
    private static CassandraContainer target = new CassandraContainer("cassandra")
            .withExposedPorts(9042)
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
//        Thread.sleep(45_000);
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
    public void allTypes() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs5.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ",
                null,
                null);
//        Thread.sleep(60_000);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    public void expressionToColumn() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs10.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT acc, v1 FROM ",
                null,
                null);
//        тут https://stackoverflow.com/questions/31290815/cassandra-extract-month-from-timestamp
//        Thread.sleep(120_000);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    public void onlyTableName() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs9.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1) FROM ",
                null,
                null);
//        Thread.sleep(120_000);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
// select id, uid, v1, v2, v3, v4, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4)  from test.t1;
    public void diffKeyspace() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs7.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ",
                null,
                null);
//        Thread.sleep(120_000);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
// select id, uid, v1, v2, v3, v4, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4)  from test.t1;
    public void partitionKeys() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs6.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, v1 FROM ",
                null,
                null);
//        Thread.sleep(60_000);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
// select id, uid, v1, v2, v3, v4, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4)  from test.t1;
    public void dataOnly() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs1.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ",
                null,
                null);
//        Thread.sleep(30_000);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
// select id, uid, v1, v2, v3, v4, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4)  from test.t1;
    public void withTtlBasedOnColumn() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs8.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(log_time) FROM ",
                null,
                null);
//        Thread.sleep(60_000);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
// select id, uid, v1, v2, v3, v4, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4)  from test.t1;
    public void withTtlOrTimestamp() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        Predicate<Row> targetPredicate = i -> (i.getLong("writetime(v1)") == 9999 &&
                i.getLong("writetime(v2)") == 9999 &&
                i.getLong("writetime(v3)") == 9999 &&
                i.getLong("writetime(v4)") == 9999);
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs2.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ",
                null,
                targetPredicate);
//        Thread.sleep(180_000);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
// select id, uid, v1, v2, v3, v4, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4)  from test.t1;
    public void withoutTTL() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        Predicate<Row> targetPredicate = i -> (i.getInt("ttl(v1)") == 0 &&
                i.getInt("ttl(v2)") == 0 &&
                i.getInt("ttl(v3)") == 0 &&
                i.getInt("ttl(v4)") == 0);
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs3.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ",
                null,
                targetPredicate);
//        Thread.sleep(200_000);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
// select id, uid, v1, v2, v3, v4, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4)  from test.t1;
    public void recordCount() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        initSourceData(sourceProperties, 256);
        initTargetData(targetProperties);
        TestResult result256 = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs4.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ",
                null,
                null);
        initSourceData(sourceProperties, 260);
        initTargetData(targetProperties);
        TestResult result260 = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs4.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ",
                null,
                null);
        initSourceData(sourceProperties, 1030);
        initTargetData(targetProperties);
        TestResult result1030 = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs4.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ",
                null,
                null);
        initSourceData(sourceProperties, 524290);
        initTargetData(targetProperties);
        TestResult result524290 = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs4.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ",
                null,
                null);
        long sourceCount = result256.sourceCount() + result260.sourceCount() + result1030.sourceCount() + result524290.sourceCount();
        long targetCount = result256.targetCount() + result260.targetCount() + result1030.targetCount() + result524290.targetCount();
        System.out.println("Source count: " + sourceCount + ", target count: " + targetCount);
        assertEquals(sourceCount, targetCount);
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
            sourceCount += countCassandra(sourceProperties, query,config.fromSchemaName() + "." + config.fromTableName(), sourcePredicate);
            targetCount += countCassandra(targetProperties, query,
                    (config.toSchemaName() == null ? config.fromSchemaName() : config.toSchemaName()) + "." +
                            (config.toTableName() == null ? config.fromTableName() : config.toTableName()),
                    targetPredicate);
        }
        return new TestResult(sourceCount, targetCount);
    }

    private static long countCassandra(Properties properties, String query, String tableName, Predicate<Row> p) {
        CSPool csPool = new CSPool(properties, 2);
        CqlSession cqlSession = csPool.getCqlSession();
        ResultSet resultSet = cqlSession.execute(query + tableName);
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

    private void initSourceData(Properties properties, int rows) {
        CSPool csPool = new CSPool(properties, 2);
        CqlSession cqlSession = csPool.getCqlSession();
        String truncateTable = "truncate test.t4";
        cqlSession.execute(truncateTable);
        String insertQuery = "insert into test.t4 (id, uid, v1, v2, v3, v4) values (:id, :uid, :v1, :v2, :v3, :v4)";
        PreparedStatement preparedStatement = cqlSession.prepare(insertQuery);
        BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.UNLOGGED);
        for (int i = 0; i < rows; i++) {
            Object[] values = new Object[]{i % 16, i, i + 10, i + 100, i + 1000, "v4" + i};
            BatchableStatement<?> statement = preparedStatement.bind(values);
            batchStatementBuilder.addStatement(statement);
            if (i % Integer.parseInt(properties.getProperty("batchSize")) == 0) {
                cqlSession.execute(batchStatementBuilder.build());
                batchStatementBuilder.clearStatements();
            }
        }
        cqlSession.execute(batchStatementBuilder.build());
        batchStatementBuilder.clearStatements();
        csPool.closeCqlSession();
    }

    private void initTargetData(Properties properties) {
        CSPool csPool = new CSPool(properties, 2);
        CqlSession cqlSession = csPool.getCqlSession();
        String truncateTable = "truncate test.t4";
        cqlSession.execute(truncateTable);
        csPool.closeCqlSession();
    }
}
