package dev.bublik.cli.oracle.clickhouse;

import dev.bublik.cli.TestResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Properties;

import static dev.bublik.cli.ContainerImageVersions.CLICKHOUSE;
import static dev.bublik.cli.TestUtils.getJdbcProperties;
import static dev.bublik.cli.TestUtils.getResultCount;
import static org.junit.jupiter.api.Assertions.*;

public class V2605Test {
    private static int rows = 20000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("./oracle/clickhouse/sql/oracle/01_init.sql");

    private static final DockerImageName CLICKHOUSE_STABLE = DockerImageName
            .parse(CLICKHOUSE)
            .asCompatibleSubstituteFor("clickhouse/clickhouse-server");

    private static ClickHouseContainer target = new ClickHouseContainer(CLICKHOUSE_STABLE)
            .withInitScript("./oracle/clickhouse/sql/allTypes.sql");

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
        Properties targetProps = getJdbcProperties(target);
        TestResult result = getResultCount(
                "./oracle/clickhouse/yaml/ora2click.yaml",
                "./oracle/clickhouse/json/ora2click.json",
                rows,
                sync,
                getJdbcProperties(source),
                targetProps);
        assertEquals(result.sourceCount(), result.targetCount());

        Properties cleanProps = new Properties();
        cleanProps.putAll(targetProps);
        cleanProps.remove("url");
        try (Connection connection =
                     DriverManager.getConnection(targetProps.getProperty("url"), cleanProps)) {

            Statement statement1 = connection.createStatement();
            ResultSet rs1 = statement1.executeQuery(
                    "select ID,A,B,C,D,ALL,LEVEL,E,T,CREATE_AT,GENDER,BYTEABLOB,TEXTCLOB,EXCLUDE_ME," +
                            "CaseSensitive,COUNTRY_ID,RAWBYTEA,JSON_LIKE,DOC,UUID,INT16_T,INT128_T,INT256_T, " +
                            "toFloat32(BFLOAT16_T) AS bfloat16_check, DD, DY, DZ from b where ID = 1");
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

            assertEquals(32767, rs1.getInt("INT16_T"));
            String expectedInt128 = "170141183460469231731687303715884105727";
            assertEquals(expectedInt128, rs1.getObject("INT128_T").toString().trim());
            String expectedInt256 = "57896044618658097711785492504343953926634992332820282019728792003956564819967";
            assertEquals(expectedInt256, rs1.getObject("INT256_T").toString().trim());

            assertEquals(123.0f, rs1.getFloat("bfloat16_check"), 0.001f);
            assertEquals(LocalDate.of(2026, 6, 18), rs1.getObject("DD", LocalDate.class));

            java.sql.Timestamp tsDY = rs1.getTimestamp("DY");
            assertNotNull(tsDY, "Поле DY не должно быть null");
            assertTrue(tsDY.toString().contains("3000-01-01"),
                    "Поле DY должно содержать 3000-01-01, но получено: " + tsDY);

            LocalDate localDateDZ = rs1.getObject("DZ", LocalDate.class);
            assertNotNull(localDateDZ, "Поле DZ не должно быть null");
            assertEquals(LocalDate.of(3000, 1, 1), localDateDZ,
                    "Поле DZ должно быть строго равно 3000-01-01");

            rs1.close();
            statement1.close();

            Statement statement2 = connection.createStatement();
            ResultSet rs2 = statement2.executeQuery(
                    "select ID,A,B,C,D,ALL,LEVEL,E,T,CREATE_AT,GENDER,BYTEABLOB,TEXTCLOB,EXCLUDE_ME," +
                            "CaseSensitive,COUNTRY_ID,RAWBYTEA,JSON_LIKE,DOC,UUID,INT16_T,INT128_T,INT256_T, " +
                            "toFloat32(BFLOAT16_T) AS bfloat16_check from b where ID = 2");
            assertTrue(rs2.next());

            assertEquals(2, rs2.getLong("ID"));

            assertEquals(900, rs2.getInt("EXCLUDE_ME"));
            assertFalse(rs2.wasNull());

            rs2.getBigDecimal("A");
            assertTrue(rs2.wasNull());
            rs2.getLong("B");
            assertTrue(rs2.wasNull());
            rs2.getString("C");
            assertTrue(rs2.wasNull());
            rs2.getString("D");
            assertTrue(rs2.wasNull());
            rs2.getString("ALL");
            assertTrue(rs2.wasNull());
            rs2.getString("LEVEL");
            assertTrue(rs2.wasNull());
            rs2.getFloat("E");
            assertTrue(rs2.wasNull());
            rs2.getTimestamp("T");
            assertTrue(rs2.wasNull());
            rs2.getTimestamp("CREATE_AT");
            assertTrue(rs2.wasNull());
            rs2.getInt("GENDER");
            assertTrue(rs2.wasNull());
            rs2.getString("BYTEABLOB");
            assertTrue(rs2.wasNull());
            rs2.getString("TEXTCLOB");
            assertTrue(rs2.wasNull());
            rs2.getString("CaseSensitive");
            assertTrue(rs2.wasNull());
            rs2.getInt("COUNTRY_ID");
            assertTrue(rs2.wasNull());
            rs2.getString("RAWBYTEA");
            assertTrue(rs2.wasNull());
            rs2.getString("JSON_LIKE");
            assertTrue(rs2.wasNull());
            rs2.getString("DOC");
            assertTrue(rs2.wasNull());
            rs2.getString("UUID");
            assertTrue(rs2.wasNull());
            rs2.getObject("INT16_T");
            assertTrue(rs2.wasNull());
            rs2.getObject("INT128_T");
            assertTrue(rs2.wasNull());
            rs2.getObject("INT256_T");
            assertTrue(rs2.wasNull());
            rs2.getFloat("bfloat16_check");
            assertTrue(rs2.wasNull());

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

            rs2.getInt("EXCLUDE_ME");
            assertTrue(rs2.wasNull());
            rs2.getBigDecimal("A");
            assertTrue(rs2.wasNull());
            rs2.getLong("B");
            assertTrue(rs2.wasNull());
            rs2.getString("C");
            assertTrue(rs2.wasNull());
            rs2.getString("D");
            assertTrue(rs2.wasNull());
            rs2.getString("ALL");
            assertTrue(rs2.wasNull());
            rs2.getString("LEVEL");
            assertTrue(rs2.wasNull());
            rs2.getFloat("E");
            assertTrue(rs2.wasNull());
            rs2.getTimestamp("T");
            assertTrue(rs2.wasNull());
            rs2.getTimestamp("CREATE_AT");
            assertTrue(rs2.wasNull());
            rs2.getInt("GENDER");
            assertTrue(rs2.wasNull());
            rs2.getString("BYTEABLOB");
            assertTrue(rs2.wasNull());
            rs2.getString("TEXTCLOB");
            assertTrue(rs2.wasNull());
            rs2.getString("CaseSensitive");
            assertTrue(rs2.wasNull());
            rs2.getInt("COUNTRY_ID");
            assertTrue(rs2.wasNull());
            rs2.getString("RAWBYTEA");
            assertTrue(rs2.wasNull());
            rs2.getString("JSON_LIKE");
            assertTrue(rs2.wasNull());
            rs2.getString("DOC");
            assertTrue(rs2.wasNull());
            rs2.getString("UUID");
            assertTrue(rs2.wasNull());

            rs2.close();
            statement2.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    @DisplayName("Upsert to ReplacingMergeTree engine")
    void expression2Column() throws Exception {
        Properties targetProps = getJdbcProperties(target);
        TestResult result = getResultCount(
                "./oracle/clickhouse/yaml/ora2click.yaml",
                "./oracle/clickhouse/json/e2c.json",
                rows,
                sync,
                getJdbcProperties(source),
                targetProps);
        assertEquals(result.sourceCount(), result.targetCount() + 1);
//        Thread.sleep(100_000);
        Properties cleanProps = new Properties();
        cleanProps.putAll(targetProps);
        cleanProps.remove("url");
        try (Connection connection =
                     DriverManager.getConnection(targetProps.getProperty("url"), cleanProps)) {

            Statement statement1 = connection.createStatement();
            ResultSet rs1 = statement1.executeQuery("select * from d final where ID = 4");
            assertTrue(rs1.next());

            assertEquals(4, rs1.getLong("ID"));
            assertEquals(200, rs1.getDouble("A"), 0.001);

            java.sql.Timestamp tsT = rs1.getTimestamp("T");
            assertNotNull(tsT);
            assertTrue(tsT.toString().contains("2026-06-18"));

            rs1.close();
            statement1.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void manualChunks() throws Exception {
        Properties targetProps = getJdbcProperties(target);
        TestResult result = getResultCount(
                "./oracle/clickhouse/yaml/ora2click.yaml",
                "./oracle/clickhouse/json/manualChunks.json",
                0,
                sync,
                getJdbcProperties(source),
                targetProps);
        assertEquals(result.sourceCount(), result.targetCount());
    }
}
