package dev.bublik.cli.oracle.clickhouse;

import dev.bublik.cli.TestResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.oracle.OracleContainer;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.util.Properties;

import static dev.bublik.cli.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class V2111Test {
    private static int rows = 20000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("./oracle/clickhouse/sql/oracle/01_init.sql");

    private static ClickHouseContainer target = new ClickHouseContainer("clickhouse/clickhouse-server:21.11-alpine")
            .withInitScript("./oracle/clickhouse/sql/allTypes2111.sql");

    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(java.util.Collections.singletonList("1521:1521"));
        source.start();
        target.setPortBindings(java.util.List.of("8123:8123", "9000:9000"));
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
    void allTypes() throws Exception {
        Properties targetProps = getFullJdbcProperties(target);
        TestResult result = getResultCount(
                "./oracle/clickhouse/yaml/ora2click.yaml",
                "./oracle/clickhouse/json/ora2click.json",
                rows,
                sync,
                getJdbcProperties(source),
                targetProps);
//        Thread.sleep(200_000);
        assertEquals(result.sourceCount(), result.targetCount());

        Properties cleanProps = new Properties();
        cleanProps.putAll(targetProps);
        cleanProps.remove("url");
        try (Connection connection =
                     DriverManager.getConnection(targetProps.getProperty("url"), cleanProps)) {

            Statement statement1 = connection.createStatement();
            ResultSet rs1 = statement1.executeQuery("select * from b where ID = 1");
            assertTrue(rs1.next());

            assertEquals(1, rs1.getLong("ID"));
            assertEquals(123456.78, rs1.getDouble("A"), 0.001);
            assertEquals(987654321L, rs1.getLong("B"));
            assertEquals(1, rs1.getInt("GENDER"));
            assertEquals(999, rs1.getInt("EXCLUDE_ME"));
            assertEquals(7, rs1.getInt("COUNTRY_ID"));

            assertEquals(100.0f, rs1.getFloat("E"), 0.001f);

            assertEquals("Y", rs1.getString("C"));
            assertEquals("NCHAR_VAL", rs1.getString("D").trim());
            assertEquals("Тестовая строка NVARCHAR2", rs1.getString("ALL"));
            assertEquals("Уровень доступа VARCHAR2", rs1.getString("LEVEL"));
            assertEquals("РеГиСтР_СиМвОлОв", rs1.getString("CaseSensitive"));

            assertEquals("DEADBEEF01020304", rs1.getString("BYTEABLOB"));
            assertEquals("Текст внутри поля CLOB", rs1.getString("TEXTCLOB"));
            assertEquals("AABBCCDDEEFF00112233445566778899", rs1.getString("RAWBYTEA"));

            assertEquals("{\"key\": \"just_string\"}", rs1.getString("JSON_LIKE"));
            assertEquals("{\"user\": \"Dmitrii\", \"role\": \"admin\"}", rs1.getString("DOC"));

            assertEquals("3e2e125a-b6c9-4f9b-9682-d21ec40564bc", rs1.getString("UUID"));

            java.sql.Timestamp tsT = rs1.getTimestamp("T");
            assertNotNull(tsT);
            assertTrue(tsT.toString().contains("2026-06-18"));

            java.sql.Timestamp tsCreateAt = rs1.getTimestamp("CREATE_AT");
            assertNotNull(tsCreateAt);
            assertTrue(tsCreateAt.toString().contains("2026-06-18"));

            rs1.close();
            statement1.close();

            Statement statement2 = connection.createStatement();
            ResultSet rs2 = statement2.executeQuery("select * from b where ID = 2");
            assertTrue(rs2.next());

            assertEquals(2, rs2.getLong("ID"));

            assertEquals(900, rs2.getInt("EXCLUDE_ME"));
            assertFalse(rs2.wasNull());

            rs2.getBigDecimal("A"); assertTrue(rs2.wasNull());
            rs2.getLong("B");       assertTrue(rs2.wasNull());
            rs2.getString("C");     assertTrue(rs2.wasNull());
            rs2.getString("D");     assertTrue(rs2.wasNull());
            rs2.getString("ALL");   assertTrue(rs2.wasNull());
            rs2.getString("LEVEL"); assertTrue(rs2.wasNull());
            rs2.getFloat("E");      assertTrue(rs2.wasNull());
            rs2.getTimestamp("T");  assertTrue(rs2.wasNull());
            rs2.getTimestamp("CREATE_AT"); assertTrue(rs2.wasNull());
            rs2.getInt("GENDER");   assertTrue(rs2.wasNull());
            rs2.getString("BYTEABLOB");    assertTrue(rs2.wasNull());
            rs2.getString("TEXTCLOB");     assertTrue(rs2.wasNull());
            rs2.getString("CaseSensitive"); assertTrue(rs2.wasNull());
            rs2.getInt("COUNTRY_ID");      assertTrue(rs2.wasNull());
            rs2.getString("RAWBYTEA");     assertTrue(rs2.wasNull());
            rs2.getString("JSON_LIKE");    assertTrue(rs2.wasNull());
            rs2.getString("DOC");          assertTrue(rs2.wasNull());
            rs2.getString("UUID");         assertTrue(rs2.wasNull());

            rs2.close();
            statement2.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void column2Column() throws Exception {
        Properties targetProps = getJdbcProperties(target);
        TestResult result = getResultCount(
                "./oracle/clickhouse/yaml/ora2click.yaml",
                "./oracle/clickhouse/json/c2c.json",
                rows,
                sync,
                getJdbcProperties(source),
                targetProps);
        assertEquals(result.sourceCount(), result.targetCount());
//        Thread.sleep(100_000);
        Properties cleanProps = new Properties();
        cleanProps.putAll(targetProps);
        cleanProps.remove("url");
        try (Connection connection =
                     DriverManager.getConnection(targetProps.getProperty("url"), cleanProps)) {

            Statement statement1 = connection.createStatement();
            ResultSet rs1 = statement1.executeQuery("select * from c where ID = 1");
            assertTrue(rs1.next());

            assertEquals(1, rs1.getLong("ID"));
            assertEquals(123456.78, rs1.getDouble("A"), 0.001);
            assertEquals("Тестовая строка NVARCHAR2", rs1.getString("ALL"));
            assertEquals("3e2e125a-b6c9-4f9b-9682-d21ec40564bc", rs1.getString("UUID"));

            rs1.close();
            statement1.close();

            Statement statement2 = connection.createStatement();
            ResultSet rs2 = statement2.executeQuery("select * from c where ID = 2");
            assertTrue(rs2.next());

            assertEquals(2, rs2.getLong("ID"));

            rs2.getInt("EXCLUDE_ME"); assertTrue(rs2.wasNull());
            rs2.getBigDecimal("A"); assertTrue(rs2.wasNull());
            rs2.getLong("B");       assertTrue(rs2.wasNull());
            rs2.getString("C");     assertTrue(rs2.wasNull());
            rs2.getString("D");     assertTrue(rs2.wasNull());
            rs2.getString("ALL");   assertTrue(rs2.wasNull());
            rs2.getString("LEVEL"); assertTrue(rs2.wasNull());
            rs2.getFloat("E");      assertTrue(rs2.wasNull());
            rs2.getTimestamp("T");  assertTrue(rs2.wasNull());
            rs2.getTimestamp("CREATE_AT"); assertTrue(rs2.wasNull());
            rs2.getInt("GENDER");   assertTrue(rs2.wasNull());
            rs2.getString("BYTEABLOB");    assertTrue(rs2.wasNull());
            rs2.getString("TEXTCLOB");     assertTrue(rs2.wasNull());
            rs2.getString("CaseSensitive"); assertTrue(rs2.wasNull());
            rs2.getInt("COUNTRY_ID");      assertTrue(rs2.wasNull());
            rs2.getString("RAWBYTEA");     assertTrue(rs2.wasNull());
            rs2.getString("JSON_LIKE");    assertTrue(rs2.wasNull());
            rs2.getString("DOC");          assertTrue(rs2.wasNull());
            rs2.getString("UUID");         assertTrue(rs2.wasNull());

            rs2.close();
            statement2.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

}
