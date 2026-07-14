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
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;

import static dev.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CassandraToCassandraTest {
    private static int rows = 50000;
    private static boolean sync = false;

    private static CassandraContainer source = new CassandraContainer("cassandra")
            .withEnv("CASSANDRA_USER", "cassandra")
            .withEnv("CASSANDRA_PASSWORD", "cassandra")
            .withEnv("CASSANDRA_USER_DEFINED_FUNCTIONS_ENABLED", "true")
            .withEnv("CASSANDRA_AUTHENTICATOR", "PasswordAuthenticator")
            .withEnv("CASSANDRA_NUM_TOKENS", "16")
            .withCopyToContainer(MountableFile.forClasspathResource("./cassandra/cassandra/conf/docker-entrypoint.sh"), "/usr/local/bin/docker-entrypoint.sh")
            .withInitScript("./cassandra/cassandra/sql/cs-init.cql")
            .withExposedPorts(9042);
    private static CassandraContainer target = new CassandraContainer("cassandra")
            .withEnv("CASSANDRA_USER", "cassandra")
            .withEnv("CASSANDRA_PASSWORD", "cassandra")
            .withEnv("CASSANDRA_USER_DEFINED_FUNCTIONS_ENABLED", "true")
            .withEnv("CASSANDRA_AUTHENTICATOR", "PasswordAuthenticator")
            .withEnv("CASSANDRA_NUM_TOKENS", "16")
            .withCopyToContainer(MountableFile.forClasspathResource("./cassandra/cassandra/conf/docker-entrypoint.sh"), "/usr/local/bin/docker-entrypoint.sh")
            .withInitScript("./cassandra/cassandra/sql/cs-init-empty.cql")
            .withExposedPorts(9042);

    @BeforeAll
    static void setUp() throws SQLException, IOException, InterruptedException {
        source.setPortBindings(Collections.singletonList("9042:9042"));
        source.start();
        target.setPortBindings(Collections.singletonList("9043:9042"));
        target.start();
    }

    @AfterAll
    static void clear() throws InterruptedException {
        source.stop();
        target.stop();
    }

    @Test
    public void filter() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs11.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, v1, v2, v3, v4 FROM ",
                null,
                null);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
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
    public void allTypesAsPartitionKeys() throws InterruptedException, IOException {
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
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
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
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    public void withTtlBasedOnColumn() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        Predicate<Row> targetPredicate = i -> (i.getInt("ttl(log_time)") > 10000000 && i.getInt("ttl(log_time)") < 160000000);
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs8.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(log_time) FROM ",
                null,
                targetPredicate);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
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
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
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
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    public void toBlob() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = sourceProperties;
        TestResult result = getResult(
                "./cassandra/cassandra/yaml/cs2cs-blob.yaml",
                "./cassandra/cassandra/json/cs2cs13.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT key, value FROM ",
                null,
                null);
//        Thread.sleep(360_000);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    public void recordCount() throws InterruptedException, IOException {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9043");
        long sourceCount = 0;
        long targetCount = 0;
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
        sourceCount += result256.sourceCount();
        targetCount += result256.targetCount();
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
/*
        initSourceData(sourceProperties, 1_000_000);
        initTargetData(targetProperties);
        TestResult result_1_000_000 = getResult(
                "./cassandra/cassandra/yaml/cs2cs.yaml",
                "./cassandra/cassandra/json/cs2cs4.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ",
                null,
                null);
        sourceCount += result_1_000_000.sourceCount();
        targetCount += result_1_000_000.targetCount();
*/
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

        App.runProcess(cp, configs, rows);

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

    private Properties getPropertiesOfCassandra(String hosts) {
        Properties properties = new Properties();
        properties.setProperty("class", "org.bublik.cassandra.storage.CassandraStorage");
        properties.setProperty("keyspace", "test");
        properties.setProperty("hosts", hosts);
        properties.setProperty("user", "cassandra");
        properties.setProperty("password", "cassandra");
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
