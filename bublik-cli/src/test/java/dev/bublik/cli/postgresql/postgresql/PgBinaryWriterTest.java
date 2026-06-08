package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.cli.TestResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

import static dev.bublik.cli.TestUtils.getJdbcProperties;
import static dev.bublik.cli.TestUtils.getResultCount;
import static org.junit.jupiter.api.Assertions.*;

public class PgBinaryWriterTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/pgBinaryWriter.sql");
    private static JdbcDatabaseContainer<?> target = source;

    @BeforeAll
    static void setUp() throws SQLException {
        MountableFile mf = MountableFile.forClasspathResource("./images/bublik.png");
        source.addFileSystemBind(mf.getResolvedPath(), "/var/lib/postgresql/bublik.png", BindMode.READ_ONLY);
        source.setPortBindings(java.util.Collections.singletonList("5432:5432"));
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
    static void clear() {
        source.stop();
        while (source.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void pgBinaryWriter() throws IOException, InterruptedException {
        Properties targetProp = getJdbcProperties(target);
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "./postgresql/postgresql/json/pgBinaryWriter.json",
                rows,
                sync,
                getJdbcProperties(source),
                targetProp);
//        Thread.sleep(90_000);
        assertEquals(result.sourceCount(), result.targetCount());

        try (Connection connection = DriverManager.getConnection(targetProp.getProperty("url"), targetProp)) {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select * from test.b where id = 1");
            assertTrue(rs.next());

            assertEquals(1, rs.getInt("id"));
            assertEquals("varchar(40)", rs.getString("name"));
            assertEquals(new java.math.BigDecimal("1"), rs.getBigDecimal("Nam"));
            assertEquals("bpchar(10)", rs.getString("ALL").trim()); // trim, так как bpchar дополняется пробелами
            assertEquals("char(10)", rs.getString("d").trim());
            assertEquals("character(40)", rs.getString("e").trim());
            assertEquals("text", rs.getString("f"));

            java.sql.Array arrayG = rs.getArray("g");
            assertNotNull(arrayG);
            assertArrayEquals(new String[]{"a", "b", "c"}, (String[]) arrayG.getArray());

            java.sql.Array arrayH = rs.getArray("h");
            assertNotNull(arrayH);
            assertArrayEquals(new String[]{"a", "b", "c"}, (String[]) arrayH.getArray());

            rs.getShort("i");
            assertTrue(rs.wasNull());

            assertEquals("{\"key\": \"json\"}", rs.getString("j"));
            assertEquals("{\"key\": \"jsonb\"}", rs.getString("jb"));

            assertEquals(1, rs.getShort("k"));
            assertEquals(2, rs.getLong("l"));
            assertEquals(3, rs.getLong("m"));
            assertEquals(4.6865f, rs.getFloat("n"), 0.0001f);
            assertEquals(5.8362d, rs.getDouble("o"), 0.0001d);
            assertEquals(294636.9362048d, rs.getDouble("p"), 0.000001d);

            assertNotNull(rs.getString("q"));
            assertTrue(rs.getBoolean("r"));

            assertArrayEquals(
                    new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF, 0x01, 0x02, 0x03, 0x04},
                    rs.getBytes("w_bytea")
            );
            assertEquals("192.168.1.55", rs.getString("x_inet"));

            @SuppressWarnings("unchecked")
            java.util.Map<String, String> hstoreMap = (java.util.Map<String, String>) rs.getObject("y_hstore");
            assertNotNull(hstoreMap);
            assertEquals("success", hstoreMap.get("status"));
            assertEquals("Postgres", hstoreMap.get("target"));
            assertEquals("Oracle", hstoreMap.get("vendor"));
            assertNull(hstoreMap.get("nullable_key")); // Проверяем, что вложенный NULL сохранился в hstore
            assertTrue(hstoreMap.containsKey("nullable_key")); // И ключ при этом присутствует

            java.sql.Array arrayZ = rs.getArray("z_bigint_arr");
            assertNotNull(arrayZ);
            assertArrayEquals(
                    new Long[]{100000000001L, 100000000002L, 100000000003L},
                    (Long[]) arrayZ.getArray()
            );

            java.sql.Array arrayAA = rs.getArray("aa_uuid_arr");
            assertNotNull(arrayAA);
            java.util.UUID[] uuids = (java.util.UUID[]) arrayAA.getArray();
            assertEquals(3, uuids.length);
            assertNotNull(uuids[0]);
            assertNotNull(uuids[1]);
            assertNotNull(uuids[2]);

            assertNotNull(rs.getDate("s_date"));
            assertNotNull(rs.getTimestamp("t_timestamp"));
            assertNotNull(rs.getTimestamp("u_timestamptz"));
            assertNotNull(rs.getTime("v_time"));

            assertEquals("2 years 3 mons 5 days 11:22:33.444555", rs.getString("ac_interval"));

            String tstzrangeStr = rs.getString("ab_tstzrange");
            assertNotNull(tstzrangeStr);
            assertTrue(tstzrangeStr.startsWith("["));
            assertTrue(tstzrangeStr.endsWith(")"));
            rs.close();

            ResultSet rsNull = statement.executeQuery("select * from test.b where id = 2");
            assertTrue(rsNull.next());
            assertEquals(2, rsNull.getInt("id"));

            rsNull.getString("name"); assertTrue(rsNull.wasNull());
            rsNull.getBigDecimal("Nam"); assertTrue(rsNull.wasNull());
            rsNull.getArray("g"); assertTrue(rsNull.wasNull());
            rsNull.getString("jb"); assertTrue(rsNull.wasNull());
            rsNull.getBytes("w_bytea"); assertTrue(rsNull.wasNull());
            rsNull.getString("ab_tstzrange"); assertTrue(rsNull.wasNull());
            rsNull.close();
            statement.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
