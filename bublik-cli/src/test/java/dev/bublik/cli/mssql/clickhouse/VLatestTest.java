package dev.bublik.cli.mssql.clickhouse;

import dev.bublik.cli.TestResult;
import dev.bublik.core.model.DummyTable;
import dev.bublik.core.model.Table;
import org.junit.jupiter.api.*;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;
import java.util.Properties;

import static dev.bublik.cli.TestUtils.getJdbcProperties;
import static dev.bublik.cli.TestUtils.getResultCount;
import static org.junit.jupiter.api.Assertions.*;

public class VLatestTest {
    private static int rows = 20000;
    private static JdbcDatabaseContainer<?> source = new MSSQLServerContainer("mcr.microsoft.com/mssql/server")
            .acceptLicense()
            .withInitScript("./mssql/clickhouse/sql/mssql.sql");

    private static final DockerImageName CLICKHOUSE_LATEST = DockerImageName
            .parse("clickhouse")
            .asCompatibleSubstituteFor("clickhouse/clickhouse-server");

    private static ClickHouseContainer target = new ClickHouseContainer(CLICKHOUSE_LATEST)
            .withInitScript("./mssql/clickhouse/sql/click.sql");

    @BeforeAll
    static void setUp() {
        source.setPortBindings(java.util.Collections.singletonList("1433:1433"));
        source.start();

        target.setPortBindings(java.util.List.of("8123:8123", "9000:9000"));
        target.start();
    }

    @AfterAll
    static void clear() {
        source.stop();
        target.stop();
    }

    @Test
    @DisplayName("Тест миграции всех типов: MS SQL Server -> ClickHouse")
    void mssqlAllTypes() throws Exception {
        Properties targetProps = getJdbcProperties(target);

        Table chunkTable = new DummyTable("test", "chunk");
        Table outboxTable = new DummyTable("test", "outbox");
        TestResult result = getResultCount(
                "./mssql/clickhouse/yaml/mssql2click.yaml",
                "./mssql/clickhouse/json/mssql2click.json",
                rows,
                false,
                getMSSQLJdbcProperties(source),
                targetProps,
                chunkTable,
                outboxTable);

//        Thread.sleep(400_000);
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
                            "toFloat32(BFLOAT16_T) AS bfloat16_check from b where ID = 1");
            assertTrue(rs1.next());

            // Ассерты для Строки №1 (Заполненная)
            assertEquals(1, rs1.getLong("ID"));
            assertEquals(123456.78, rs1.getDouble("A"), 0.001);
            assertEquals(987654321L, rs1.getLong("B"));
            assertEquals(1, rs1.getInt("GENDER"));
            assertEquals(999, rs1.getInt("EXCLUDE_ME"));
            assertEquals(7, rs1.getInt("COUNTRY_ID"));

            assertEquals(123.45f, rs1.getFloat("E"), 0.001f);
            assertEquals("Y", rs1.getString("C"));
            assertEquals("NCHAR_VAL", rs1.getString("D").trim());
            assertEquals("Тестовая строка NVARCHAR2", rs1.getString("ALL"));
            assertEquals("Text VARCHAR2", rs1.getString("LEVEL"));
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

            rs1.close();
            statement1.close();

            // Ассерты для Строки №2 (Nullable / Пустая)
            Statement statement2 = connection.createStatement();
            ResultSet rs2 = statement2.executeQuery(
                    "select ID,A,B,C,D,ALL,LEVEL,E,T,CREATE_AT,GENDER,BYTEABLOB,TEXTCLOB,EXCLUDE_ME," +
                            "CaseSensitive,COUNTRY_ID,RAWBYTEA,JSON_LIKE,DOC,UUID,INT16_T,INT128_T,INT256_T, " +
                            "toFloat32(BFLOAT16_T) AS bfloat16_check from b where ID = 2");
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
            rs2.getObject("INT16_T");  assertTrue(rs2.wasNull());
            rs2.getObject("INT128_T"); assertTrue(rs2.wasNull());
            rs2.getObject("INT256_T"); assertTrue(rs2.wasNull());
            rs2.getFloat("bfloat16_check"); assertTrue(rs2.wasNull());

            rs2.close();
            statement2.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Properties getMSSQLJdbcProperties(JdbcDatabaseContainer<?> db) {
        Properties properties = new Properties();
        properties.setProperty("url", db.getJdbcUrl());
        properties.setProperty("databaseName", "test");
        properties.setProperty("user", db.getUsername());
        properties.setProperty("password", db.getPassword());
        properties.setProperty("encrypt", "true");
        properties.setProperty("trustServerCertificate", "true");
        return properties;
    }
}
