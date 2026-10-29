package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.core.constants.TransferMode;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.Storage2Storage;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageService;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.postgres.model.PGTable;
import dev.bublik.postgres.storage.PostgresStorage;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

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
                10_000,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    void allTypes() throws Exception {
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg_snapshot.yaml",
                "./postgresql/postgresql/json/allTypes.json",
                20_000,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        assertEquals(result.sourceCount(), result.targetCount());
        TestResult result2 = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg_snapshot.yaml",
                "./postgresql/postgresql/json/allTypes.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result2.sourceCount());
        System.out.println("target count: " + (result2.targetCount() - result.targetCount()));
        Properties targetProp = getJdbcProperties(target);
        try (Connection conn = DriverManager.getConnection(targetProp.getProperty("url"), targetProp);
             Statement stmt = conn.createStatement()) {
            stmt.execute("truncate table target");
        }
        assertEquals(result.sourceCount(), result2.targetCount() - result.targetCount());
    }

    @Test
    void sumCountHashWithParallelLoad() throws Exception {
        CountDownLatch longTxStarted = new CountDownLatch(1);
        CountDownLatch xminCaptured = new CountDownLatch(1);

        CompletableFuture<Void> longTransactionTask = CompletableFuture.runAsync(() -> {
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
                conn.setAutoCommit(false);

                try (PreparedStatement psUpdate = conn.prepareStatement(
                        "UPDATE source2 SET \"Primary\" = 'long_tx_mod_' || id, numeric = numeric + 100 WHERE id <= 10");
                     PreparedStatement psInsert = conn.prepareStatement(
                             "INSERT INTO source2 (id, int2, int4, int8, smallint, bigint, numeric, float8, \"Primary\") " +
                                     "VALUES (?, 1, 1, 1, 1, 1, 100.0, 100.0, ?)")) {

                    psUpdate.executeUpdate();

                    int startId = 20_000_000;
                    for (int i = 0; i < 10; i++) {
                        psInsert.setInt(1, startId + i);
                        psInsert.setString(2, "long_tx_user_" + i);
                        psInsert.addBatch();
                    }
                    psInsert.executeBatch();

                    longTxStarted.countDown();
                    xminCaptured.await();
                    conn.commit();

                } catch (Exception e) {
                    conn.rollback();
                    throw e;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        longTxStarted.await();

        final long xMin;
        try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint")) {
            assertTrue(rs.next());
            xMin = rs.getLong(1);

            xminCaptured.countDown();
        }

        longTransactionTask.get();

        CompletableFuture<Void> backgroundLoadTask = CompletableFuture.runAsync(() -> {
            Random random = new Random();

            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword());
                 PreparedStatement psUpdate = conn.prepareStatement(
                         "UPDATE source2 SET \"Primary\" = substr(md5(random()::text), 1, 100), numeric = numeric + ? " +
                                 "WHERE id = (SELECT id FROM source2 OFFSET ? LIMIT 1)");
                 PreparedStatement psInsert = conn.prepareStatement(
                         "INSERT INTO source2 (id, int2, int4, int8, smallint, bigint, numeric, float8, \"Primary\") " +
                                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {

                conn.setAutoCommit(true);
                int nextId = 10_000_000;

                int iterations = 0;
                while (iterations < 10_000) {
                    if (random.nextBoolean()) {
                        psUpdate.setDouble(1, random.nextDouble());
                        psUpdate.setInt(2, random.nextInt(100));
                        psUpdate.executeUpdate();
                    } else {
                        psInsert.setInt(1, nextId++);
                        psInsert.setShort(2, (short) random.nextInt(10));
                        psInsert.setInt(3, random.nextInt(100));
                        psInsert.setLong(4, random.nextLong(1000));
                        psInsert.setShort(5, (short) random.nextInt(10));
                        psInsert.setLong(6, random.nextLong(1000));
                        psInsert.setDouble(7, random.nextDouble());
                        psInsert.setDouble(8, random.nextDouble());
                        psInsert.setString(9, "user_" + nextId);
                        psInsert.executeUpdate();
                    }
                    iterations++;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        TestResult result = null;
        try {
            result = getResultCount(
                    "./postgresql/postgresql/yaml/pg2pg_sum_hash.yaml",
                    "./postgresql/postgresql/json/sumCount.json",
                    10_000,
                    sync,
                    getJdbcProperties(source),
                    getJdbcProperties(target),
                    TransferMode.COPY);
        } catch (Exception e) {
            System.out.println(getStackTrace(e));
        }

        backgroundLoadTask.get();

        try (Connection connection = DriverManager.getConnection(getJdbcProperties(source).getProperty("url"), getJdbcProperties(source));
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop table " + chunkTable.getSchemaName() + "." + chunkTable.getTableName());
            statement.executeUpdate("drop table " + outboxTable.getSchemaName() + "." + outboxTable.getTableName());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        ConnectionProperty connectionProperty = connectionProperty(
                PostgresToPostgresTest.class.getResourceAsStream("/postgresql/postgresql/yaml/pg2pg_sum_hash.yaml"));
        List<Config> configsTmp = getConfigs(
                PostgresToPostgresTest.class.getResourceAsStream("/postgresql/postgresql/json/sumCount.json"));
        Config configFirst = configsTmp.getFirst();
        List<Config> configs = new ArrayList<>();
        String fetchWhereClause = "xmin::text::bigint >= " + xMin;
        System.out.println(fetchWhereClause);
        Config config = Config
                .builder()
                .from(configFirst.fromSchemaName(), configFirst.fromTableName())
                .to(configFirst.toSchemaName(), configFirst.toTableName())
                .columnToColumn(configFirst.columnToColumn())
                .fetchWhereClause(fetchWhereClause)
                .build();
        configs.add(config);
        StorageService.init(connectionProperty, configs, 50_000, TransferMode.UPSERT);

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
                             "FROM source2")) {
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

        // Жесткая сверка результатов
        assertEquals(sCnt, tCnt, "COUNT не совпал!");
        assertEquals(sId, tId, "SUM(id) не совпал!");
        assertEquals(sInt2, tInt2, "SUM(int2) не совпал!");
        assertEquals(sInt4, tInt4, "SUM(int4) не совпал!");
        assertEquals(sInt8, tInt8, "SUM(int8) не совпал!");
        assertEquals(sSmallint, tSmallint, "SUM(smallint) не совпал!");
        assertEquals(sBigint, tBigint, "SUM(bigint) не совпал!");
        assertEquals(sNum, tNum, 0.000001f, "SUM(numeric) не совпал!");
        assertEquals(sFloat, tFloat, 0.000001d, "SUM(float8) не совпал!");
        assertEquals(sPrimary, tPrimary, "Хеш строк Primary не совпал!");

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
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
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
        System.out.println(ex.getMessage());

        try (Connection connection = DriverManager.getConnection(sourceProperties.getProperty("url"), sourceProperties);
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "drop table " + chunkTable.getSchemaName() + "." + chunkTable.getTableName());
            System.out.println("Table " + chunkTable.getTableName() + " dropped");
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

        SQLException ex = assertThrows(SQLException.class, () ->
                StorageService.init(property, configs, rows, chunkTable, outboxTable));
        assertTrue(ex.getMessage().contains("Partitioned tables are not supported"));
        System.out.println(ex.getMessage());
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
    void pkFailure() throws Exception {
        Table tChunk = new PGTable.Builder("public", "t_chunk").build();
        Table tOutbox = new PGTable.Builder("public", "t_outbox").build();
        ConnectionProperty connectionProperty = ConnectionProperty.builder()
                .threadCount(2)
                .addFromProperty("url", "jdbc:postgresql://localhost:5432/postgres?options=-c%20enable_indexscan=off%20-c%20enable_indexonlyscan=off%20-c%20enable_bitmapscan=off")
                .addFromProperty("user", "test")
                .addFromProperty("password", "test")
                .addToProperty("url", "jdbc:postgresql://localhost:5432/postgres")
                .addToProperty("user", "test")
                .addToProperty("password", "test")
                .build();
        List<Config> configs = new ArrayList<>(Collections.singleton(
                Config.builder()
                        .from("public", "pk_failure")
                        .to("test", "pk_failure")
                        .build()
        ));

        try {
            StorageService.init(connectionProperty, configs, 1_000, tChunk, tOutbox);
        } catch (Exception e) {
            assertTrue(getStackTrace(e).contains("duplicate key value violates unique constraint"));
        }

        Properties sourceProperties = new Properties(connectionProperty.getToProperty());
        try (Connection connection = DriverManager.getConnection(sourceProperties.getProperty("url"), sourceProperties);
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "delete from test.pk_failure where id = 1");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try {
            StorageService.init(connectionProperty, configs, 0, tChunk, tOutbox);
        } catch (Exception e) {
            assertFalse(getStackTrace(e).contains("duplicate key value violates unique constraint"));
        }
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
            assertTrue(getStackTrace(e).contains("violates not-null constraint"));
            System.out.println(e.getMessage());
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

//        Thread.sleep(900_000);
        TestResult result2 = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "postgresql/postgresql/json/notNullFailure.json",
                0,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result2.sourceCount());
        System.out.println("target count: " + result2.targetCount());
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