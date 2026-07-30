package dev.bublik.cli.oracle.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.cli.TestUtils;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.util.Properties;

import static dev.bublik.cli.TestUtils.getJdbcProperties;
import static dev.bublik.cli.TestUtils.getResultCount;
import static org.junit.jupiter.api.Assertions.*;

public class OracleToPostgresTest {
    private static int rows = 20000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("./oracle/postgres/sql/oracle/01_init.sql");
    private static JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./oracle/postgres/sql/pg-init-empty.sql");

    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(java.util.Collections.singletonList("1521:1521"));
        source.start();
        MountableFile mf = MountableFile.forClasspathResource("./images/bublik.png");
        target.addFileSystemBind(mf.getResolvedPath(), "/var/lib/postgresql/bublik.png", BindMode.READ_ONLY);
        target.setPortBindings(java.util.Collections.singletonList("5432:5432"));
        target.start();
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
    void pgBinaryWriter() throws Exception {
        Properties targetProp = getJdbcProperties(target);
        TestResult result = getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/pgBinaryWriter.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
//        Thread.sleep(200_000);
        assertEquals(result.sourceCount(), result.targetCount());

        try (Connection connection = DriverManager.getConnection(targetProp.getProperty("url"), targetProp)) {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select * from test.a where id = 1");
            assertTrue(rs.next());

            assertEquals(1, rs.getInt("id"));
            assertEquals(new java.math.BigDecimal("123456.78"), rs.getBigDecimal("a"));
            assertEquals(new java.math.BigDecimal("987654321"), rs.getBigDecimal("b"));

            assertEquals("Y", rs.getString("c"));
            assertEquals("NCHAR_VAL ", rs.getString("d")); // В Postgres char(10) сохраняет хвостовые пробелы
            assertEquals("Тестовая строка NVARCHAR2", rs.getString("ALL"));
            assertEquals("Уровень доступа VARCHAR2", rs.getString("LEVEL"));

            assertEquals(100f, rs.getFloat("e"), 0.001f);

            assertNotNull(rs.getTimestamp("t"));
            assertNotNull(rs.getTimestamp("create_at"));

            assertEquals(1, rs.getShort("gender"));
            assertEquals("Текст внутри поля CLOB", rs.getString("textclob"));
            assertEquals(999, rs.getInt("exclude_me"));

            assertEquals("РеГиСтР_СиМвОлОв", rs.getString("CaseSensitive"));
            assertEquals(7, rs.getInt("country_id"));

            assertArrayEquals(
                    new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF, 0x01, 0x02, 0x03, 0x04},
                    rs.getBytes("byteablob")
            );
            assertArrayEquals(
                    new byte[]{
                            (byte) 0xAA, (byte) 0xBB, (byte) 0xCC, (byte) 0xDD,
                            (byte) 0xEE, (byte) 0xFF, 0x00, 0x11,
                            0x22, 0x33, 0x44, 0x55,
                            0x66, 0x77, (byte) 0x88, (byte) 0x99
                    },
                    rs.getBytes("rawbytea")
            );

            assertEquals("{\"key\": \"just_string\"}", rs.getString("json_like"));

            String docJson = rs.getString("doc");
            assertNotNull(docJson);
            assertTrue(docJson.contains("\"user\": \"Dmitrii\""));
            assertTrue(docJson.contains("\"role\": \"admin\""));

            assertEquals("3e2e125a-b6c9-4f9b-9682-d21ec40564bc", rs.getString("uuid").trim());
            assertEquals( java.sql.Date.valueOf(java.time.LocalDate.now()) , rs.getDate("dd"));

            ResultSet rsNull = statement.executeQuery("select * from test.a where id = 2");
            assertTrue(rsNull.next());
            assertEquals(2, rsNull.getInt("id"));

            rsNull.getBigDecimal("a"); assertTrue(rsNull.wasNull());
            rsNull.getBigDecimal("b"); assertTrue(rsNull.wasNull());
            rsNull.getString("c");     assertTrue(rsNull.wasNull());
            rsNull.getString("d");     assertTrue(rsNull.wasNull());
            rsNull.getString("ALL");   assertTrue(rsNull.wasNull());
            rsNull.getString("LEVEL"); assertTrue(rsNull.wasNull());
            rsNull.getFloat("e");      assertTrue(rsNull.wasNull());
            rsNull.getTimestamp("t");  assertTrue(rsNull.wasNull());
            rsNull.getTimestamp("create_at"); assertTrue(rsNull.wasNull());
            rsNull.getShort("gender"); assertTrue(rsNull.wasNull());
            rsNull.getBytes("byteablob"); assertTrue(rsNull.wasNull());
            rsNull.getString("textclob"); assertTrue(rsNull.wasNull());
            rsNull.getInt("exclude_me");  assertTrue(rsNull.wasNull());
            rsNull.getString("CaseSensitive"); assertTrue(rsNull.wasNull());
            rsNull.getInt("country_id");  assertTrue(rsNull.wasNull());
            rsNull.getBytes("rawbytea");  assertTrue(rsNull.wasNull());
            rsNull.getString("json_like"); assertTrue(rsNull.wasNull());
            rsNull.getString("doc");      assertTrue(rsNull.wasNull());
            rsNull.getString("uuid");     assertTrue(rsNull.wasNull());
            rsNull.getString("dd");     assertTrue(rsNull.wasNull());

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void columnOrder() throws Exception {
        TestResult result = getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/columnOrder.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    void parted() throws Exception {
        TestResult result = TestUtils.getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/parted.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.targetCount(), result.sourceCount());
    }

    @Test
    void leftJoin() throws Exception {
        TestResult result = TestUtils.getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/leftJoin.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
//        Thread.sleep(200_000);
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.targetCount(), result.sourceCount());
    }

    void columnFromMany() throws Exception {
        TestResult result = TestUtils.getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/columnFromMany.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
//        Thread.sleep(200_000);
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.targetCount(), result.sourceCount());
    }

    @Test
    void interval() throws Exception {
        TestResult result = TestUtils.getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/interval.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.targetCount(), result.sourceCount());
    }
}
