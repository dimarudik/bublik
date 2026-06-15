package dev.bublik.cli.postgresql.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import dev.bublik.cli.App;
import dev.bublik.cli.TestResult;
import dev.bublik.cli.TestUtils;
import dev.bublik.cli.addons.Utils;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static dev.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PostgresToCassandraTtlTest {

    private static int rows = 50000;
    private static boolean sync = false;

    private static JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./postgresql/cassandra/sql/pg-init-cache.sql")
            .withExposedPorts(5432);
    private static CassandraContainer target = new CassandraContainer("cassandra")
            .withEnv("CASSANDRA_USER", "cassandra")
            .withEnv("CASSANDRA_PASSWORD", "cassandra")
            .withEnv("CASSANDRA_AUTHENTICATOR", "PasswordAuthenticator")
            .withEnv("CASSANDRA_NUM_TOKENS", "16")
            //.withCopyToContainer(MountableFile.forClasspathResource("./cassandra/cassandra/conf/docker-entrypoint.sh"), "/usr/local/bin/docker-entrypoint.sh")
            .withInitScript("./postgresql/cassandra/sql/cs-init-cache.cql")
            .withEnv("TZ", "Europe/Moscow")
            .withExposedPorts(9042);

    @BeforeAll
    static void setUp() throws Exception {
        source.setPortBindings(Collections.singletonList("5432:5432"));
        source.start();
        target.setPortBindings(Collections.singletonList("9042:9042"));
        target.start();

        // Ожидание готовности PostgreSQL
        while (!source.isRunning()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // Явная инициализация схемы Cassandra после старта контейнера
        // Это гарантирует применение изменений из cs-init-cache.cql при переиспользовании контейнера
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(java.net.InetSocketAddress.createUnresolved("localhost", 9042))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials("cassandra", "cassandra")
                .build()) {
            session.execute("DROP KEYSPACE IF EXISTS test");
            session.execute("CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' }");
            session.execute("CREATE TABLE test.ttl_check (id bigint, offer_id bigint, flags tinyint, primary key (id))");
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

    @DisplayName("TTL из кэша - проверка значений TTL для разных сервисов")
    @Test
    public void ttlFromCacheValues() throws IOException {
        Properties sourceProperties = getJdbcProperties(source);
        Properties targetProperties = getJdbcPropertiesOfCassandra(target);

        TestResult result = getResult(
                "./postgresql/cassandra/yaml/pg2cs-cache.yaml",
                "./postgresql/cassandra/json/pg2cs-cache.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);

        assertEquals(result.sourceCount(), result.targetCount());

        // Проверка TTL для разных записей
        Map<Long, Integer> ttlMap = getTargetTtlByOfferId(targetProperties, "SELECT id, offer_id, ttl(offer_id) as ttl FROM test.ttl_check");
        System.out.println("TTL map: " + ttlMap);

        // offer_id=12345,23456: HOTELS_POSTPAY - ~2 года (63072000 секунд)
        // offer_id=34567,45678: AVIA/CONCERT - ~6 месяцев (15768000 секунд)
        // offer_id=56789: HEALTH - ~1 месяц (2592000 секунд)
        // offer_id=99999: отсутствует в кэше - 2 года (63072000 секунд)
        // offer_id=77777: отрицательный TTL - 1 неделя (604800 секунд)
    }

    @DisplayName("TTL из кэша - проверка записи с отсутствующим ключом в кэше")
    @Test
    public void ttlFromCacheMissingKey() throws IOException {
        Properties sourceProperties = getJdbcProperties(source);
        Properties targetProperties = getJdbcPropertiesOfCassandra(target);

        TestResult result = getResult(
                "./postgresql/cassandra/yaml/pg2cs-cache.yaml",
                "./postgresql/cassandra/json/pg2cs-cache.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);

        assertEquals(result.sourceCount(), result.targetCount());

        // Проверка записи с offer_id=99999 (отсутствует в кэше)
        // В логах должно быть предупреждение: "TTL из кэша не получен для offer_id=99999"
        Map<Long, Integer> ttlMap = getTargetTtlByOfferId(targetProperties,
                "SELECT id, offer_id, ttl(offer_id) as ttl FROM test.ttl_check WHERE offer_id = 99999 ALLOW FILTERING");

        System.out.println("TTL for missing key: " + ttlMap);
        // Ожидается TTL по умолчанию 2 года (63072000 секунд)
        assertEquals(1, ttlMap.size());
        Integer ttl = ttlMap.values().iterator().next();
        assert ttl != null && ttl > 63000000 && ttl <= 63072000 : "TTL для отсутствующего ключа должен быть 2 года, фактически: " + ttl;
    }

    @DisplayName("TTL из кэша - проверка записи с отрицательным TTL")
    @Test
    public void ttlFromCacheNegative() throws IOException {
        Properties sourceProperties = getJdbcProperties(source);
        Properties targetProperties = getJdbcPropertiesOfCassandra(target);

        TestResult result = getResult(
                "./postgresql/cassandra/yaml/pg2cs-cache.yaml",
                "./postgresql/cassandra/json/pg2cs-cache.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);

        assertEquals(result.sourceCount(), result.targetCount());

        // Проверка записи с отрицательным TTL (должен быть установлен в 1 неделю)
        // offer_id=77777 имеет отрицательный TTL, должен быть заменен на 1 неделю (604800 секунд)
        Map<Long, Integer> ttlMap = getTargetTtlByOfferId(targetProperties,
                "SELECT id, offer_id, ttl(offer_id) as ttl FROM test.ttl_check WHERE offer_id = 77777 ALLOW FILTERING");

        System.out.println("TTL for negative value: " + ttlMap);
        // Ожидается TTL = 1 неделя (604800 секунд)
        assertEquals(1, ttlMap.size());
        Integer ttl = ttlMap.values().iterator().next();
        assert ttl != null && ttl >= 604700 && ttl <= 604900 : "TTL для отрицательного значения должен быть 1 неделя, фактически: " + ttl;
    }

    @DisplayName("flags из кэша - проверка значений flags для разных сервисов")
    @Test
    public void flagsFromCacheValues() throws IOException {
        Properties sourceProperties = getJdbcProperties(source);
        Properties targetProperties = getJdbcPropertiesOfCassandra(target);

        TestResult result = getResult(
                "./postgresql/cassandra/yaml/pg2cs-cache.yaml",
                "./postgresql/cassandra/json/pg2cs-cache.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);

        assertEquals(result.sourceCount(), result.targetCount());

        // Проверка TTL для разных записей
        Map<Long, Byte> ttlMap = getTargetByOfferId(targetProperties, "SELECT id, offer_id, flags FROM test.ttl_check");
        assertEquals(3, ttlMap.values().stream().filter(f -> f != 0).count());
        System.out.println("flags map: " + ttlMap);
        // offer_id=12345,23456,34567: - excluded
    }


    public static TestResult getResult(String connectionPropertyFile,
            String mappingFile,
            int rows,
            boolean sync,
            Properties sourceProperties,
            Properties targetProperties) throws IOException {
        ConnectionProperty cp = Utils.connectionProperty(TestUtils.getFilePath(connectionPropertyFile));

        List<Config> configs = getConfigs(TestUtils.getFilePath(mappingFile));
        App.runProcess(cp, configs, rows, sync, null);

        long sourceCount = 0;
        long targetCount = 0;
        for (Config config : configs) {
            sourceCount += countRows(sourceProperties,
                    "SELECT count(1) FROM " + config.fromSchemaName() + "." + config.fromTableName());
            targetCount += countCassandra(targetProperties,
                    "SELECT id, offer_id FROM ",
                    (config.toSchemaName() == null ? config.fromSchemaName() : config.toSchemaName()) + "." +
                    (config.toTableName() == null ? config.fromTableName() : config.toTableName()),
                    null);
        }
        return new TestResult(sourceCount, targetCount);
    }

    private static long countCassandra(Properties properties, String query, String tableName, Object p) {
        CqlSession cqlSession = CqlSession
                .builder()
                .addContactPoint(java.net.InetSocketAddress.createUnresolved("localhost", 9042))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials(properties.getProperty("user"), properties.getProperty("password"))
                .build();

        String q = query + tableName;
        System.out.println(q);
        ResultSet resultSet = cqlSession.execute(q);
        long rowCount = 0;
        for (Row row : resultSet) {
            rowCount++;
        }
        cqlSession.close();
        return rowCount;
    }

    private static long countRows(Properties p, String query) {
        try (java.sql.Connection connection =
                java.sql.DriverManager.getConnection(p.getProperty("url"), p.getProperty("user"), p.getProperty("password"))) {
            java.sql.Statement statement = connection.createStatement();
            java.sql.ResultSet resultSet = statement.executeQuery(query);
            resultSet.next();
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<Long, Integer> getTargetTtlByOfferId(Properties properties, String query) {
        CqlSession cqlSession = CqlSession
                .builder()
                .addContactPoint(java.net.InetSocketAddress.createUnresolved("localhost", 9042))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials(properties.getProperty("user"), properties.getProperty("password"))
                .build();

        ResultSet resultSet = cqlSession.execute(query);
        Map<Long, Integer> ttlMap = new HashMap<>(4);
        for (Row row : resultSet) {
            long offerId = row.getLong("offer_id");
            int ttl = row.getInt("ttl");
            ttlMap.put(offerId, ttl);
        }
        cqlSession.close();
        return ttlMap;
    }

    public Map<Long, Byte> getTargetByOfferId(Properties properties, String query) {
        CqlSession cqlSession = CqlSession
                .builder()
                .addContactPoint(java.net.InetSocketAddress.createUnresolved("localhost", 9042))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials(properties.getProperty("user"), properties.getProperty("password"))
                .build();

        ResultSet resultSet = cqlSession.execute(query);
        Map<Long, Byte> ttlMap = new HashMap<>(4);
        for (Row row : resultSet) {
            long offerId = row.getLong("offer_id");
            byte flags = row.getByte("flags");
            ttlMap.put(offerId, flags);
        }
        cqlSession.close();
        return ttlMap;
    }

    private Properties getJdbcProperties(JdbcDatabaseContainer<?> db) {
        Properties properties = new Properties();
        properties.setProperty("url", db.getJdbcUrl());
        properties.setProperty("user", db.getUsername());
        properties.setProperty("password", db.getPassword());
        return properties;
    }

    private Properties getJdbcPropertiesOfCassandra(CassandraContainer target) {
        Properties properties = new Properties();
        properties.setProperty("class", "dev.bublik.cassandra.storage.CassandraStorage");
        properties.setProperty("keyspace", "test");
        properties.setProperty("hosts", "localhost:9042");
        properties.setProperty("user", "cassandra");
        properties.setProperty("password", "cassandra");
        properties.setProperty("datacenter", "datacenter1");
        properties.setProperty("batchSize", "256");
        return properties;
    }
}
