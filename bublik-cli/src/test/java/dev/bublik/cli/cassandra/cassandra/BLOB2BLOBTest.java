package dev.bublik.cli.cassandra.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import dev.bublik.cli.App;
import dev.bublik.cli.TestResult;
import dev.bublik.cli.TestUtils;
import dev.bublik.cli.addons.Utils;
import org.bublik.cassandra.storage.CSPool;
import org.bublik.core.model.Config;
import org.bublik.core.model.ConnectionProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.BindMode;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;

import static dev.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;

//@Disabled
public class BLOB2BLOBTest {
    private static int rows = 50000;
    private static boolean sync = false;

    private static CassandraContainer source = new CassandraContainer("cassandra")
            .withExposedPorts(9042)
            .withEnv("CASSANDRA_USER_DEFINED_FUNCTIONS_ENABLED", "true")
            .withInitScript("./cassandra/cassandra/sql/cs-init-blob.cql");
    private static CassandraContainer target = source;

    @BeforeAll
    static void setUp() throws SQLException, IOException, InterruptedException {
        MountableFile mf = MountableFile.forClasspathResource("./cassandra/cassandra/conf/docker-entrypoint.sh");
        source.addFileSystemBind(mf.getResolvedPath(), "/usr/local/bin/docker-entrypoint.sh", BindMode.READ_ONLY);
        source.setPortBindings(Collections.singletonList("9042:9042"));
        source.start();
        while (!source.isRunning()) {
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
    public void toBLOB() throws InterruptedException, IOException {
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
//        Thread.sleep(90_000);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
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
}
