package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.service.StorageService;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Properties;

import static dev.bublik.cli.App.getConfigs;
import static dev.bublik.cli.TestUtils.*;
import static dev.bublik.cli.addons.Utils.connectionProperty;
import static dev.bublik.core.util.Utils.getStackTrace;
import static org.junit.jupiter.api.Assertions.*;

public class PostgresToPostgresTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./postgresql/postgresql/sql/pg-init.sql");
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
    void orderByOptimisation() throws Exception {
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "./postgresql/postgresql/json/orderByOptimisation.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    void allTypes() throws Exception {
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "./postgresql/postgresql/json/allTypes.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        assertEquals(result.sourceCount(), result.targetCount());
        TestResult result2 = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "./postgresql/postgresql/json/allTypes.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        assertEquals(result.sourceCount(), result2.targetCount() - result.targetCount());
    }

    @Test
    void sumCountHash() throws Exception {
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "./postgresql/postgresql/json/sumCount.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        assertEquals(result.sourceCount(), result.targetCount());

        long sCnt, sId, sInt2, sInt4, sInt8, sSmallint, sBigint;
        double sNum, sFloat;
        long sPrimary;
        try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(1) as cnt, SUM(id) as id, sum(int2) as int2, sum(int4) as int4, " +
                             "sum(int8) as int8, sum(smallint) as smallint, sum(bigint) as bigint, " +
                             "sum(numeric) as num, sum(float8) as float8, " +
                             "sum(hashtext(\"Primary\")::bigint + hashtext(reverse(\"Primary\"))::bigint) as primary " +
                             "FROM \"Source\"")) {
            assertTrue(rs.next());
            sCnt = rs.getLong("cnt");
            sId = rs.getLong("id");
            sInt2 = rs.getLong("int2");
            sInt4 = rs.getLong("int4");
            sInt8 = rs.getLong("int8");
            sSmallint = rs.getLong("smallint");
            sBigint = rs.getLong("bigint");
            sNum = rs.getDouble("num");
            sFloat = rs.getFloat("float8");
            sPrimary = rs.getLong("primary");
        }

        long tCnt, tId, tInt2, tInt4, tInt8, tSmallint, tBigint;
        double tNum, tFloat;
        long tPrimary;
        try (Connection conn = DriverManager.getConnection(target.getJdbcUrl(), target.getUsername(), target.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(1) as cnt, SUM(id) as id, sum(int2) as int2, sum(int4) as int4, " +
                             "sum(int8) as int8, sum(smallint) as smallint, sum(bigint) as bigint, " +
                             "sum(num) as num, sum(float8) as float8, " +
                             "sum(hashtext(\"Primary\")::bigint + hashtext(reverse(\"Primary\"))::bigint) as primary " +
                             "FROM target2")) {
            assertTrue(rs.next());
            tCnt = rs.getLong("cnt");
            tId = rs.getLong("id");
            tInt2 = rs.getLong("int2");
            tInt4 = rs.getLong("int4");
            tInt8 = rs.getLong("int8");
            tSmallint = rs.getLong("smallint");
            tBigint = rs.getLong("bigint");
            tNum = rs.getDouble("num");
            tFloat = rs.getFloat("float8");
            tPrimary = rs.getLong("primary");
        }

        assertEquals(sCnt, tCnt);
        assertEquals(sId, tId);
        assertEquals(sInt2, tInt2);
        assertEquals(sInt4, tInt4);
        assertEquals(sInt8, tInt8);
        assertEquals(sSmallint, tSmallint);
        assertEquals(sBigint, tBigint);
        assertEquals(sNum, tNum, 0.000001f);
        assertEquals(sFloat, tFloat, 0.000001d);
        assertEquals(sPrimary, tPrimary);
    }

    @Test
    void columnOrder() throws Exception {
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "./postgresql/postgresql/json/columnOrder.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    void chunkTableAlreadyExists() throws Exception {
        Properties sourceProperties = getJdbcProperties(source);
        try (Connection connection = DriverManager.getConnection(sourceProperties.getProperty("url"), sourceProperties);
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "create table " + chunkTable.getSchemaName() + "." + chunkTable.getTableName() + " (id int primary key)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        SQLException ex = assertThrows(SQLException.class, () ->
                getResult(
                        "./postgresql/postgresql/yaml/pg2pg.yaml",
                        "./postgresql/postgresql/json/columnOrder.json",
                        rows,
                        sync,
                        sourceProperties,
                        getJdbcProperties(target),
                        chunkTable,
                        outboxTable));
        assertTrue(ex.getMessage().contains("relation \"" + chunkTable.getTableName() + "\" already exists"));

        try (Connection connection = DriverManager.getConnection(sourceProperties.getProperty("url"), sourceProperties);
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "drop table " + chunkTable.getSchemaName() + "." + chunkTable.getTableName());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void isNotPartitioned() throws IOException, InterruptedException, SQLException {
        ConnectionProperty property = connectionProperty(
                PostgresToPostgresTest.class.getResourceAsStream("/postgresql/postgresql/yaml/pg2pg.yaml"));
        List<Config> configs = getConfigs(
                PostgresToPostgresTest.class.getResourceAsStream("/postgresql/postgresql/json/isNotPartitioned.json"));

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                        StorageService.init(property, configs, rows, chunkTable, outboxTable));
        assertTrue(ex.getMessage().contains("Partitioned tables are not supported"));
    }

    @Test
    void targetTableNotExists() throws Exception {
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "./postgresql/postgresql/json/targetTableNotExists.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    void notNullFailure() throws Exception {
        try {
            TestResult result = getResultCount(
                    "./postgresql/postgresql/yaml/pg2pg.yaml",
                    "postgresql/postgresql/json/notNullFailure.json",
                    rows,
                    sync,
                    getJdbcProperties(source),
                    getJdbcProperties(target));
        } catch (Exception e) {
//            System.out.println("Ошибка: " + getStackTrace(e));
            assertTrue(getStackTrace(e).contains("violates not-null constraint"));
        }
        String jdbcUrl = source.getJdbcUrl();
        String username = source.getUsername();
        String password = source.getPassword();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            PreparedStatement ps = connection.prepareStatement("update public.not_null_failure set name = 'a' where id = 1000");
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        TestResult result2 = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "postgresql/postgresql/json/notNullFailure.json",
                0,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        assertEquals(result2.sourceCount(), result2.targetCount());
    }

    @Test
    void serialColumn() throws Exception {
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "postgresql/postgresql/json/serialColumn.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    void emptyChunks() throws Exception {
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "postgresql/postgresql/json/emptySourceTable.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }
}