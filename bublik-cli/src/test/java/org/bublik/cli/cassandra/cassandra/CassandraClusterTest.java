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
import org.testcontainers.cassandra.delegate.CassandraDatabaseDelegate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;
import static org.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CassandraClusterTest {
    private static final int rows = 10000;
    private static final boolean sync = false;

    private static final Network network = Network.newNetwork();
    private static final String dockerImage = "cassandra:4.1.10";
    private static final String sourceHost1 = "source1";
    private static final String sourceHost2 = "source2";
    private static final String sourceHost3 = "source3";
    private static final String targetHost1 = "target1";
    private static final String targetHost2 = "target2";
    private static final String targetHost3 = "target3";
    private static final int[] listenPorts = {9042};
    private static final Integer[] ports = {7000, 7199, 9042};
    private static final Map<String, String> sourceEnv = envMap("source", "DC1", "RACK1",
            sourceHost1, sourceHost2, sourceHost3);
    private static final Map<String, String> targetEnv = envMap("target", "DC2", "RACK2",
            targetHost1, targetHost2, targetHost3);
    private static final Cluster sourceCluster = new Cluster(network, dockerImage,
            List.of(sourceHost1, sourceHost2, sourceHost3), ports, listenPorts, sourceEnv);
    private static final Cluster targetCluster = new Cluster(network, dockerImage,
            List.of(targetHost1, targetHost2, targetHost3), ports, listenPorts, targetEnv);
    private static final List<GenericContainer<?>> sourceContainers = sourceCluster.initCLuster();
    private static final List<GenericContainer<?>> targetContainers = targetCluster.initCLuster();

    @BeforeAll
    static void setUp() throws InterruptedException, IOException {
        MountableFile sourceInit = MountableFile.forClasspathResource("./cassandra/cassandra/sql/cs-init-rf3.cql");
//        MountableFile cassandraEnv = MountableFile.forClasspathResource("./cassandra/cassandra/sql/cassandra-env.sh");
        MountableFile targetInit = MountableFile.forClasspathResource("./cassandra/cassandra/sql/cs-init-empty-rf3.cql");
        GenericContainer<?> sourceLeader = sourceContainers.getFirst();
//        sourceLeader.addFileSystemBind(cassandraEnv.getResolvedPath(), "/etc/cassandra/cassandra-env.sh", BindMode.READ_WRITE);
//        sourceLeader.execInContainer("cqlsh", "-f", "/init.cql");
        GenericContainer<?> targetLeader = targetContainers.getFirst();
        final int[] port = {9042};
        sourceContainers.forEach(container -> {
            container.setPortBindings(singletonList(port[0] + ":9042"));
            port[0]++;
            container.start();
        });
        do {
            boolean allRunning = sourceContainers.stream().allMatch(Container::isRunning);
            if (allRunning) {
                sourceLeader.copyFileToContainer(sourceInit, "/init.cql");
                (new CassandraDatabaseDelegate(sourceLeader)).execute(null, "/init.cql", -1, false, false);
                break;
            } else {
                Thread.sleep(200);
            }
        } while (true);
        targetContainers.forEach(container -> {
            container.setPortBindings(singletonList(port[0] + ":9042"));
            port[0]++;
            container.start();
        });
        do {
            boolean allRunning = targetContainers.stream().allMatch(Container::isRunning);
            if (allRunning) {
                targetLeader.copyFileToContainer(targetInit, "/init.cql");
                (new CassandraDatabaseDelegate(targetLeader)).execute(null, "/init.cql", -1, false, false);
                break;
            } else {
                Thread.sleep(200);
            }
        } while (true);
    }

    @AfterAll
    static void tearDown() {
        sourceContainers.forEach(container -> {
                    try {
                        container.stop();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }


    @Test
    public void recordCount() throws InterruptedException, IOException {
        long sourceCount = 0;
        long targetCount = 0;
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042,localhost:9043,localhost:9044", "DC1");
        Properties targetProperties = getPropertiesOfCassandra("localhost:9045,localhost:9046,localhost:9047", "DC2");
        initSourceData(sourceProperties, 256);
        initTargetData(targetProperties);
        TestResult result256 = getResult(
                "./cassandra/cassandra/yaml/cs2cs-rf3.yaml",
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
                "./cassandra/cassandra/yaml/cs2cs-rf3.yaml",
                "./cassandra/cassandra/json/cs2cs4.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ",
                null,
                null);
        sourceCount += result260.sourceCount();
        targetCount += result260.targetCount();
        initSourceData(sourceProperties, 1030);
        initTargetData(targetProperties);
        TestResult result1030 = getResult(
                "./cassandra/cassandra/yaml/cs2cs-rf3.yaml",
                "./cassandra/cassandra/json/cs2cs4.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ",
                null,
                null);
        sourceCount += result1030.sourceCount();
        targetCount += result1030.targetCount();
        initSourceData(sourceProperties, 1048590);
        initTargetData(targetProperties);
        TestResult result1048590 = getResult(
                "./cassandra/cassandra/yaml/cs2cs-rf3.yaml",
                "./cassandra/cassandra/json/cs2cs4.json",
                rows,
                sync,
                sourceProperties,
                targetProperties,
                "SELECT id, uid, ttl(v1), ttl(v2), ttl(v3), ttl(v4), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ",
                null,
                null);
        sourceCount += result1048590.sourceCount();
        targetCount += result1048590.targetCount();
        System.out.println("Source count: " + sourceCount + ", target count: " + targetCount);
//        Thread.sleep(300_000);
        assertEquals(sourceCount, targetCount);
    }


    public static Map<String, String> envMap(String clusterName, String dataCenter, String rack, String... hosts) {
        Map<String, String> srcEnv = new HashMap<>();
        String seeds = String.join(",", hosts);
        srcEnv.put("JVM_OPTS", "-Xms384M -Xmx384M");
        srcEnv.put("CASSANDRA_SEEDS", seeds);
        srcEnv.put("CASSANDRA_CLUSTER_NAME", clusterName);
        srcEnv.put("CASSANDRA_DC", dataCenter);
        srcEnv.put("CASSANDRA_RACK", rack);
        srcEnv.put("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch");
        srcEnv.put("CASSANDRA_NUM_TOKENS", "16");
        return srcEnv;
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
            targetCount += countCassandra(targetProperties, query, config.toSchemaName() + "." + config.toTableName(), targetPredicate);
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

    private Properties getPropertiesOfCassandra(String hosts, String datacenter) {
        Properties properties = new Properties();
        properties.setProperty("class", "org.bublik.cassandra.storage.CassandraStorage");
        properties.setProperty("keyspace", "test");
        properties.setProperty("hosts", hosts);
        properties.setProperty("user", "test");
        properties.setProperty("password", "test");
        properties.setProperty("datacenter", datacenter);
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
            Object[] values = new Object[]{i, i, i + 10, i + 100, i + 1000, "v4" + i};
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
