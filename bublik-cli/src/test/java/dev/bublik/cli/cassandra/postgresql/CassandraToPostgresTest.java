package dev.bublik.cli.cassandra.postgresql;

import dev.bublik.cli.App;
import dev.bublik.cli.TestUtils;
import dev.bublik.cli.addons.Utils;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import static dev.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.*;

public class CassandraToPostgresTest {
    private static int rows = 10_000;
    private static boolean sync = false;
    private static CassandraContainer source = new CassandraContainer("cassandra")
            .withEnv("CASSANDRA_USER", "cassandra")
            .withEnv("CASSANDRA_PASSWORD", "cassandra")
            .withEnv("CASSANDRA_USER_DEFINED_FUNCTIONS_ENABLED", "true")
            .withEnv("CASSANDRA_AUTHENTICATOR", "PasswordAuthenticator")
            .withEnv("CASSANDRA_NUM_TOKENS", "16")
            .withCopyToContainer(MountableFile.forClasspathResource("./cassandra/cassandra/conf/docker-entrypoint.sh"), "/usr/local/bin/docker-entrypoint.sh")
            .withInitScript("./cassandra/postgresql/sql/cs-init.cql")
            .withExposedPorts(9042);
    private static JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./cassandra/postgresql/sql/pg-init.sql");

    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(java.util.Collections.singletonList("9042:9042"));
        source.start();
        target.setPortBindings(java.util.Collections.singletonList("5432:5432"));
        target.start();
        while (!source.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @BeforeEach
    void setUpZone() {
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow"));
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
    public void primitiveTypes() throws InterruptedException, IOException {
        Properties sourceProp = getJdbcPropertiesOfCassandra(source);
        Properties targetProp = getJdbcProperties(target);
        boolean result = getResult(
                "./cassandra/postgresql/yaml/cs2pg.yaml",
                "./cassandra/postgresql/json/cs2pg.json",
                rows,
                sync,
                sourceProp,
                targetProp);
        assertTrue(result);

        try (Connection connection = DriverManager.getConnection(targetProp.getProperty("url"), targetProp)) {
            Statement statement = connection.createStatement();
            ResultSet rs1 = statement.executeQuery("select * from public.t1 where id = 1");
            assertTrue(rs1.next());
            assertEquals(1, rs1.getInt("id"));
            assertEquals("ascii", rs1.getString("a"));
            assertEquals(9999999999L, rs1.getLong("b"));
            assertTrue(rs1.getBoolean("d"));
            assertEquals(java.sql.Date.valueOf("2018-01-01"), rs1.getDate("e"));
            assertEquals("12:00:00", rs1.getTime("n").toString());
            java.time.Instant sourceInstant = java.time.Instant.parse("2025-12-02T00:00:00.001Z");
            java.time.LocalDateTime expectedLocal = sourceInstant.atZone(java.time.ZoneId.systemDefault()).toLocalDateTime();
            java.sql.Timestamp expectedTimestamp = java.sql.Timestamp.valueOf(expectedLocal);
            assertEquals(expectedTimestamp, rs1.getTimestamp("o"));
            java.time.ZonedDateTime sourceO1 = java.time.ZonedDateTime.parse(
                    "2025-12-02T00:00:00.001+08:00",
                    java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
            );
            java.time.LocalDateTime expectedO1Local = sourceO1
                    .withZoneSameInstant(java.time.ZoneId.systemDefault())
                    .toLocalDateTime();
            java.sql.Timestamp expectedO1Timestamp = java.sql.Timestamp.valueOf(expectedO1Local);
            assertEquals(expectedO1Timestamp, rs1.getTimestamp("o1"),
                    "Абсолютное время в колонке o1 не совпадает с учетом часового пояса!");
            assertEquals(new java.math.BigDecimal("123.456"), rs1.getBigDecimal("f"));
            assertEquals(123.456d, rs1.getDouble("g"), 0.0001d);
            assertEquals(123.456f, rs1.getFloat("i"), 0.001f);
            assertEquals(123, rs1.getInt("k"));
            assertEquals(123, rs1.getShort("l"));
            assertEquals(123, rs1.getShort("q"));
            assertEquals("127.0.0.1", rs1.getString("j"));
            assertEquals("text", rs1.getString("m"));
            assertEquals("varchar", rs1.getString("s"));
            assertEquals(123456789L, rs1.getLong("t"));
            byte[] expectedBytes = new byte[]{
                    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                    0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
                    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18
            };
            assertArrayEquals(expectedBytes, rs1.getBytes("c"));
            java.util.UUID expectedUuid = java.util.UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f");
            assertEquals(expectedUuid, rs1.getObject("p", java.util.UUID.class));
            assertEquals(expectedUuid, rs1.getObject("r", java.util.UUID.class));

            String uJson = rs1.getString("u");
            assertNotNull(uJson);
            assertTrue(uJson.contains("\"fromSchemaName\": \"test\""));
            String mmapJson = rs1.getString("mmap");
            assertNotNull(mmapJson);
            assertTrue(mmapJson.contains("\"k1\": \"v1\""));
            assertTrue(mmapJson.contains("\"k3\": \"v2\""));
            java.sql.Array set1Array = rs1.getArray("set1");
            assertNotNull(set1Array);
            java.util.List<String> setValues = java.util.Arrays.asList((String[]) set1Array.getArray());
            assertTrue(setValues.containsAll(java.util.Arrays.asList("k1", "k2", "k3")));
            java.sql.Array list1Array = rs1.getArray("list1");
            assertNotNull(list1Array);
            assertArrayEquals(new String[]{"k1", "k2", "k3"}, (String[]) list1Array.getArray());
            assertEquals("Active", rs1.getString("status"));
            rs1.close();

            ResultSet rs2 = statement.executeQuery("select * from public.t1 where id = 2");
            assertTrue(rs2.next());
            assertEquals(2, rs2.getInt("id"));
            assertEquals("a", rs2.getString("a"));
            assertEquals("Closed", rs2.getString("status"));
            rs2.getBigDecimal("b"); assertTrue(rs2.wasNull());
            rs2.getBytes("c");      assertTrue(rs2.wasNull());
            rs2.getTimestamp("o");  assertTrue(rs2.wasNull());
            rs2.getArray("list1");  assertTrue(rs2.wasNull());
            rs2.close();

            ResultSet rs3 = statement.executeQuery("select * from public.t1 where id = 3");
            assertTrue(rs3.next());
            assertEquals(3, rs3.getInt("id"));
            rs3.getString("a");      assertTrue(rs3.wasNull());
            rs3.getString("status"); assertTrue(rs3.wasNull());
            rs3.getBigDecimal("f");  assertTrue(rs3.wasNull());
            rs3.getArray("set1");    assertTrue(rs3.wasNull());
            rs3.close();
            statement.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
//        Thread.sleep(190_000);
    }

    public static boolean getResult(String connectionPropertyFile,
                                    String mappingFile,
                                    int rows,
                                    boolean sync,
                                    Properties sourceProperties,
                                    Properties targetProperties) throws IOException {
        ConnectionProperty cp = Utils.connectionProperty(TestUtils.getFilePath(connectionPropertyFile));
        List<Config> configs = getConfigs(TestUtils.getFilePath(mappingFile));

        App.runProcess(cp, configs, rows, sync);

        return true;
    }

    private static Properties getJdbcProperties(JdbcDatabaseContainer<?> db) {
        Properties properties = new Properties();
        properties.setProperty("url", db.getJdbcUrl());
        properties.setProperty("user", db.getUsername());
        properties.setProperty("password", db.getPassword());
        return properties;
    }

    private Properties getJdbcPropertiesOfCassandra(CassandraContainer target) {
        Properties properties = new Properties();
        properties.setProperty("class", "org.bublik.cassandra.storage.CSPoolStorage");
        properties.setProperty("keyspace", "test");
        properties.setProperty("user", "cassandra");
        properties.setProperty("password", "cassandra");
        properties.setProperty("datacenter", "datacenter1");
        properties.setProperty("batchSize", "256");
        return properties;
    }
}
