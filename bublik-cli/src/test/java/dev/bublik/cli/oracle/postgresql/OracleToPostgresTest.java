package dev.bublik.cli.oracle.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.cli.TestUtils;
import dev.bublik.core.constants.TransferMode;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.service.StorageService;
import dev.bublik.oracle.storage.OracleStorage;
import dev.bublik.postgres.storage.PostgresStorage;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static dev.bublik.cli.TestUtils.getJdbcProperties;
import static dev.bublik.cli.TestUtils.getResultCount;
import static dev.bublik.core.util.Utils.getStackTrace;
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
        MountableFile mf1 = MountableFile.forClasspathResource("./oracle/postgres/sql/oracle/00.sql");
        source.addFileSystemBind(mf1.getResolvedPath(), "/container-entrypoint-initdb.d/00.sql", BindMode.READ_ONLY);
        source.setPortBindings(java.util.Collections.singletonList("1521:1521"));
        source.start();
        MountableFile mf2 = MountableFile.forClasspathResource("./images/bublik.png");
        target.addFileSystemBind(mf2.getResolvedPath(), "/var/lib/postgresql/bublik.png", BindMode.READ_ONLY);
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
    void ignorePrimaryKeyFailuresWithCopyAndUpsert() throws Exception {
        ExecutorService testExecutor = Executors.newFixedThreadPool(3);

        Future<Long> workLoad1 = testExecutor.submit(() -> {
            long updateCount = 0;
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), "bublik", "bublik")) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "update test.pk_failures_s  " +
                                "set name = 'Trx new...', updated_at = sysdate " +
                                "where id between ? and ?")) {
                    Random random = new Random();
                    for (int i = 0; i < 300; i++) {
                        int r = random.nextInt(1, 200_001);
                        ps.setInt(1, r);
                        ps.setInt(2, r + 2);
                        updateCount += ps.executeUpdate();
                        Thread.sleep(1);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return updateCount;
        });

        Future<Long> workLoad2 = testExecutor.submit(() -> {
            long updateCount = 0;
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), "bublik", "bublik")) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "update test.pk_failures_s  " +
                                "set name = 'Trx new...', updated_at = sysdate " +
                                "where id between ? and ?")) {
                    Random random = new Random();
                    for (int i = 0; i < 300; i++) {
                        int r = random.nextInt(1, 10_000);
                        ps.setInt(1, r);
                        ps.setInt(2, r + 599);
                        updateCount += ps.executeUpdate();
                        Thread.sleep(1);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return updateCount;
        });

        Future<Long> workLoad3 = testExecutor.submit(() -> {
            long updateCount = 0;
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test")) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "insert into test.pk_failures_s (id, name, updated_at) values (?, 'Trx new 2...', sysdate) ")) {
                    for (int i = 0; i < 5_000; i++) {
                        ps.setInt(1, 300_000 + i);
                        updateCount += ps.executeUpdate();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return updateCount;
        });

        TestResult result = getResultCount(
                "./oracle/postgres/yaml/ora2pgPKFailures.yaml",
                "./oracle/postgres/json/ignorePrimaryKetFailures.json",
                20_000,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        String fetchWhereClause = "ORA_ROWSCN >= " +
                result.storage2Storage().source().unwrap(OracleStorage.class).getOraRowScn();
        boolean isSuccessful = result.storage2Storage().source().isSuccessful();
        System.out.println(fetchWhereClause + " isSuccessful: " + isSuccessful);

        Long res1 = workLoad1.get();
        System.out.println("Updated by workload 1: " + res1);
        Long res2 = workLoad2.get();
        System.out.println("Updated by workload 2: " + res2);
        Long res3 = workLoad3.get();
        System.out.println("Inserted by workload 3: " + res3);
        testExecutor.shutdown();


        ConnectionProperty connectionProperty = new ConnectionProperty.Builder()
                .threadCount(2)
                .addFromProperty("url", "jdbc:oracle:thin:@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=freepdb1)))")
                .addFromProperty("user", "bublik")
                .addFromProperty("password", "bublik")
                .addToProperty("url", target.getJdbcUrl().replace("?loggerLevel=OFF", ""))
                .addToProperty("user", target.getUsername())
                .addToProperty("password", target.getPassword())
                .build();
        List<Config> configs = new ArrayList<>(Collections.singleton(Config
                .builder()
                .from("test", "pk_failures_s")
                .to("test", "pk_failures_t")
                .build()));
        List<Config> configsWithFilter = new ArrayList<>(Collections.singleton(Config
                .builder()
                .from("test", "pk_failures_s")
                .to("test", "pk_failures_t")
                .fetchWhereClause(fetchWhereClause)
                .build()));
        if (isSuccessful)
            StorageService.init(connectionProperty, configsWithFilter, 20_000, TransferMode.UPSERT);
        else {
            StorageService.init(connectionProperty, configs, TransferMode.UPSERT);
            StorageService.init(connectionProperty, configsWithFilter, 20_000, TransferMode.UPSERT);
        }

        long sCnt, sId, sTxmin, sName, sUpdatedAt;
        try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword());
             Statement stmt1 = conn.createStatement();
             ResultSet rs1 = stmt1.executeQuery(
                     "SELECT COUNT(1) as cnt, SUM(id) as id, sum(txmin) as txmin, " +
                             "SUM(TO_NUMBER(SUBSTR(STANDARD_HASH(NVL(name, 'NULL'), 'MD5'), 1, 8), 'XXXXXXXX')) AS name, " +
                             "SUM(TO_NUMBER(SUBSTR(STANDARD_HASH(NVL(TO_CHAR(updated_at, 'YYYY-MM-DD HH24:MI:SS.FF6'), 'NULL'), 'MD5'), 1, 8), 'XXXXXXXX')) AS updated_at " +
                             "FROM test.pk_failures_s")) {
            assertTrue(rs1.next());
            sCnt = rs1.getLong("cnt");
            sId = rs1.getLong("id");
            sTxmin = rs1.getLong("txmin");
            sName = rs1.getLong("name");
            sUpdatedAt = rs1.getLong("updated_at");
        }

        long tCnt, tId, tTxmin, tName, tUpdatedAt;
        try (Connection conn = DriverManager.getConnection(target.getJdbcUrl(), target.getUsername(), target.getPassword());
             Statement stmt2 = conn.createStatement();
             ResultSet rs2 = stmt2.executeQuery(
                     "SELECT COUNT(1) as cnt, SUM(id) as id, sum(txmin) as txmin, " +
                             "SUM(('x' || substr(md5(COALESCE(name, 'NULL')), 1, 8))::bit(32)::bigint) AS name, " +
                             "SUM(('x' || substr(md5(COALESCE(to_char(updated_at, 'YYYY-MM-DD HH24:MI:SS.US'), 'NULL')), 1, 8))::bit(32)::bigint) AS updated_at " +
                             "FROM pk_failures_t")) {
            assertTrue(rs2.next());
            tCnt = rs2.getLong("cnt");
            tId = rs2.getLong("id");
            tTxmin = rs2.getLong("txmin");
            tName = rs2.getLong("name");
            tUpdatedAt = rs2.getLong("updated_at");
        }

        System.out.println("COUNT: " + sCnt + " " + tCnt);
        System.out.println("id: " + sId + " " + tId);
        System.out.println("xmin: " + sTxmin + " " + tTxmin);
        System.out.println("name: " + sName + " " + tName);
        System.out.println("updated_at: " + sUpdatedAt + " " + tUpdatedAt);

        assertEquals(sCnt, tCnt, "COUNT не совпал!");
        assertEquals(sId, tId, "id не совпал!");
        assertEquals(sTxmin, tTxmin, "xmin не совпал!");
        assertEquals(sName, tName, "name не совпал!");
        assertEquals(sUpdatedAt, tUpdatedAt, "updated_at не совпал!");

        try (Connection conn = DriverManager.getConnection(target.getJdbcUrl(), target.getUsername(), target.getPassword());
             Statement stmt1 = conn.createStatement()) {
            stmt1.execute("truncate table test.pk_failures_t");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    void pgBinaryWriter() throws Exception {
//        Thread.sleep(600_000);
        Properties targetProp = getJdbcProperties(target);
        TestResult result = getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/pgBinaryWriter.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
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
    void iot() throws Exception {
        try {
            TestResult result = getResultCount(
                    "./oracle/postgres/yaml/ora2pg.yaml",
                    "./oracle/postgres/json/iot.json",
                    rows,
                    sync,
                    getJdbcProperties(source),
                    getJdbcProperties(target));
        } catch (SQLException e) {
            assertTrue(getStackTrace(e).contains("Index-Organized Tables (IOT) are not supported"));
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
//        Thread.sleep(900_000);
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    void parted() throws Exception {
        try {
            TestResult result = TestUtils.getResultCount(
                    "./oracle/postgres/yaml/ora2pg.yaml",
                    "./oracle/postgres/json/parted.json",
                    rows,
                    sync,
                    getJdbcProperties(source),
                    getJdbcProperties(target));
        } catch (Exception e) {
            assertTrue(getStackTrace(e).contains("violates not-null constraint"));
        }
        String jdbcUrl = source.getJdbcUrl();
        String username = source.getUsername();
        String password = source.getPassword();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            PreparedStatement ps = connection.prepareStatement("update test.parted set name = 'name' where name is null");
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        TestResult result = TestUtils.getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/parted.json",
                0,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
//        Thread.sleep(900_000);
        assertEquals(result.targetCount(), result.sourceCount());
    }

    @Test
    void subParted() throws Exception {
        TestResult result = getResultCount(
                "./oracle/postgres/yaml/ora2pg.yaml",
                "./oracle/postgres/json/subParted.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount() + " target count: " + result.targetCount());
//        Thread.sleep(900_000);
        assertEquals(result.sourceCount(), result.targetCount());
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
