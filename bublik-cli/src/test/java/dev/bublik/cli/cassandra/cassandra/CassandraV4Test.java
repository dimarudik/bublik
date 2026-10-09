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
import org.junit.jupiter.api.Disabled;
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

public class CassandraV4Test {
    private static int rows = 50000;
    private static boolean sync = false;

    private static CassandraContainer source = new CassandraContainer("cassandra:4.1.8")
            .withEnv("CASSANDRA_USER", "cassandra")
            .withEnv("CASSANDRA_PASSWORD", "cassandra")
            .withEnv("CASSANDRA_USER_DEFINED_FUNCTIONS_ENABLED", "true")
            .withEnv("CASSANDRA_AUTHENTICATOR", "PasswordAuthenticator")
            .withEnv("CASSANDRA_NUM_TOKENS", "16")
            .withCopyToContainer(MountableFile.forClasspathResource("./cassandra/cassandra/conf/docker-entrypoint.sh"), "/usr/local/bin/docker-entrypoint.sh")
            .withInitScript("./cassandra/cassandra/sql/cs-init.cql")
            .withExposedPorts(9042);
    private static CassandraContainer target = new CassandraContainer("cassandra:4.1.8")
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
//        Thread.sleep(600_000);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
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

    private static TestResult getResult(String connectionPropertyFile,
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
}
