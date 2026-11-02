package dev.bublik.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.bublik.core.constants.TransferMode;
import dev.bublik.core.model.Config;
import dev.bublik.core.storage.Storage;
import dev.bublik.postgres.storage.PostgresStorage;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class PostgresMigrationTest {
    static final JdbcDatabaseContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres"));

    static HikariDataSource sourceDataSource;
    static HikariDataSource targetDataSource;
    static int threadCount = 5;

    @BeforeAll
    static void beforeAll() throws Exception {
        postgres.start();

        HikariConfig sourceConfig = new HikariConfig();
        sourceConfig.setJdbcUrl(postgres.getJdbcUrl());
        sourceConfig.setUsername(postgres.getUsername());
        sourceConfig.setPassword(postgres.getPassword());
        sourceConfig.setMaximumPoolSize(threadCount);
        sourceDataSource = new HikariDataSource(sourceConfig);

        HikariConfig targetConfig = new HikariConfig();
        targetConfig.setJdbcUrl(postgres.getJdbcUrl());
        targetConfig.setUsername(postgres.getUsername());
        targetConfig.setPassword(postgres.getPassword());
        targetConfig.setMaximumPoolSize(threadCount);
        targetDataSource = new HikariDataSource(targetConfig);

        try (Connection conn = sourceDataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE source_users (id INT PRIMARY KEY, name VARCHAR(100), updated_at TIMESTAMP)");
            stmt.execute("CREATE TABLE target_users (id INT PRIMARY KEY, name VARCHAR(100), updated_at TIMESTAMP)");

            stmt.execute("INSERT INTO source_users (id, name, updated_at) VALUES " +
                    "(1, 'Alice', '2026-10-14 10:00:00'), " +
                    "(2, 'Bob', '2026-10-14 10:00:00'), " +
                    "(3, 'Charlie', '2026-10-14 10:00:00')");
        }
    }

    @AfterEach
    void afterEach() throws Exception {
        try (Connection conn = targetDataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE target_users");
        }
    }

    @AfterAll
    static void afterAll() {
        if (sourceDataSource != null) sourceDataSource.close();
        if (targetDataSource != null) targetDataSource.close();
        postgres.stop();
    }

    @Test
    void testOnlyTableNames() throws Exception {
        Storage sourceStorage = new PostgresStorage.Builder(sourceDataSource)
                .fetchSize(100)
                .build();
        Storage targetStorage = new PostgresStorage.Builder(targetDataSource)
                .build();

        List<Config> configs = new ArrayList<>();
        Config config = Config.builder()
                .from("public", "source_users")
                .to("public", "target_users")
                .build();
        configs.add(config);

        sourceStorage.start(targetStorage, configs, 1000);

        try (Connection conn = targetDataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*), MIN(name) FROM target_users")) {

            assertTrue(rs.next());
            int count = rs.getInt(1);
            String firstUser = rs.getString(2);

            assertEquals(3, count, "Количество перенесенных строк должно быть равно 3");
            assertEquals("Alice", firstUser, "Данные внутри строк должны совпадать");
        }

        assertFalse(sourceDataSource.isClosed());
        assertFalse(targetDataSource.isClosed());
    }

    @Test
    void testColumnToColumn() throws Exception {
        Storage sourceStorage = new PostgresStorage.Builder(sourceDataSource)
                .build();
        Storage targetStorage = new PostgresStorage.Builder(targetDataSource)
                .build();

        Map<String, String> columnToColumn = new LinkedHashMap<>();
        columnToColumn.put("id", "id");
        columnToColumn.put("name", "name");

        List<Config> configs = new ArrayList<>();
        Config config = Config.builder()
                .from("public", "source_users")
                .to("public", "target_users")
                .columnToColumn(columnToColumn)
                .build();
        configs.add(config);

        sourceStorage.start(targetStorage, configs, 1000);

        try (Connection conn = targetDataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*), MIN(name) FROM target_users")) {

            assertTrue(rs.next());
            int count = rs.getInt(1);
            String firstUser = rs.getString(2);

            assertEquals(3, count, "Количество перенесенных строк должно быть равно 3");
            assertEquals("Alice", firstUser, "Данные внутри строк должны совпадать");
        }

        assertFalse(sourceDataSource.isClosed());
        assertFalse(targetDataSource.isClosed());
    }

    @Test
    void testExpressionToColumn() throws Exception {
        Storage sourceStorage = new PostgresStorage.Builder(sourceDataSource)
                .build();
        Storage targetStorage = new PostgresStorage.Builder(targetDataSource)
                .build();

        Map<String, String> expressionToColumn = new LinkedHashMap<>();
        expressionToColumn.put("s.id as id", "id");
        expressionToColumn.put("s.name as name", "name");

        List<Config> configs = new ArrayList<>();
        Config config = Config.builder()
                .from("public", "source_users")
                .to("public", "target_users")
                .fromTableAlias("s")
                .expressionToColumn(expressionToColumn)
                .build();
        configs.add(config);

        sourceStorage.start(targetStorage, configs, 1000);

        try (Connection conn = targetDataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*), MIN(name) FROM target_users")) {

            assertTrue(rs.next());
            int count = rs.getInt(1);
            String firstUser = rs.getString(2);

            assertEquals(3, count, "Количество перенесенных строк должно быть равно 3");
            assertEquals("Alice", firstUser, "Данные внутри строк должны совпадать");
        }

        assertFalse(sourceDataSource.isClosed());
        assertFalse(targetDataSource.isClosed());
    }

    @Test
    void testColumnToColumnExpressionToColumn() throws Exception {
        Storage sourceStorage = new PostgresStorage.Builder(sourceDataSource)
                .build();
        Storage targetStorage = new PostgresStorage.Builder(targetDataSource)
                .build();

        Map<String, String> columnToColumn = new LinkedHashMap<>();
        columnToColumn.put("id", "id");

        Map<String, String> expressionToColumn = new LinkedHashMap<>();
        expressionToColumn.put("s.name as name", "name");

        List<Config> configs = new ArrayList<>();
        Config config = Config.builder()
                .from("public", "source_users")
                .to("public", "target_users")
                .fromTableAlias("s")
                .columnToColumn(columnToColumn)
                .expressionToColumn(expressionToColumn)
                .build();
        configs.add(config);

        sourceStorage.start(targetStorage, configs, 1000);

        try (Connection conn = targetDataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*), MIN(name) FROM target_users")) {

            assertTrue(rs.next());
            int count = rs.getInt(1);
            String firstUser = rs.getString(2);

            assertEquals(3, count, "Количество перенесенных строк должно быть равно 3");
            assertEquals("Alice", firstUser, "Данные внутри строк должны совпадать");
        }

        assertFalse(sourceDataSource.isClosed());
        assertFalse(targetDataSource.isClosed());

        sourceStorage.closeStorage();
        targetStorage.closeStorage();

        assertFalse(sourceDataSource.isClosed());
        assertFalse(targetDataSource.isClosed());
    }

    @Test
    void testDeltaUpsertMode() throws Exception {
        try (Storage sourceStorage = new PostgresStorage.Builder(sourceDataSource).build();
             Storage targetStorage = new PostgresStorage.Builder(targetDataSource).build()) {

            List<Config> initialConfigs = new ArrayList<>();
            initialConfigs.add(Config.builder()
                    .from("public", "source_users")
                    .to("public", "target_users")
                    .build());

            sourceStorage.start(targetStorage, initialConfigs, 1000, TransferMode.COPY);

            try (Connection conn = sourceDataSource.getConnection(); Statement stmt = conn.createStatement()) {
                stmt.execute("UPDATE source_users SET name = 'Bobby', updated_at = '2026-10-14 12:00:00' WHERE id = 2");
                stmt.execute("INSERT INTO source_users (id, name, updated_at) VALUES (4, 'David', '2026-10-14 12:00:00')");
            }

        }

        try (Storage sourceStorage = new PostgresStorage.Builder(sourceDataSource).build();
             Storage targetStorage = new PostgresStorage.Builder(targetDataSource).build()) {

            List<Config> deltaConfigs = new ArrayList<>();
            deltaConfigs.add(Config.builder()
                    .from("public", "source_users")
                    .to("public", "target_users")
                    .fetchWhereClause("updated_at >= '2026-10-14 11:00:00'")
                    .build());

            sourceStorage.start(targetStorage, deltaConfigs, 1000, TransferMode.UPSERT);

            try (Connection conn = targetDataSource.getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT id, name FROM target_users ORDER BY id")) {

                // Запись 1 (id=1): Не менялась, должна остаться нетронутой
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("Alice", rs.getString("name"));

                // Запись 2 (id=2): Должна быть атомарно обновлена (Upsert: Bob -> Bobby)
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("id"));
                assertEquals("Bobby", rs.getString("name"), "Имя Боба должно быть обновлено на Bobby через DO UPDATE");

                // Запись 3 (id=3): Не менялась, должна остаться нетронутой
                assertTrue(rs.next());
                assertEquals(3, rs.getInt("id"));
                assertEquals("Charlie", rs.getString("name"));

                // Запись 4 (id=4): Должна успешно добавиться на фазе дельты (Upsert: Insert David)
                assertTrue(rs.next());
                assertEquals(4, rs.getInt("id"));
                assertEquals("David", rs.getString("name"), "Новый пользователь David должен быть добавлен в таргет");

                // Проверяем, что итоговое количество строк на приемнике строго равно 4
                assertFalse(rs.next(), "Всего в целевой таблице должно быть ровно 4 записи");
            }
        }
    }

    @Test
    void testUpsertFailureWithoutPrimaryKey() throws Exception {
        // Создаем временную тестовую таблицу на источнике и приемнике без PK колонок
        try (Connection conn = sourceDataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE public.no_pk_table (val VARCHAR(10))");
            stmt.execute("INSERT INTO public.no_pk_table (val) VALUES ('test')");
        }
        try (Connection conn = targetDataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE public.no_pk_table2 (val VARCHAR(10))");
        }

        Storage sourceStorage = new PostgresStorage.Builder(sourceDataSource).build();
        Storage targetStorage = new PostgresStorage.Builder(targetDataSource).build();

        List<Config> configs = new ArrayList<>();
        configs.add(Config.builder()
                .from("public", "no_pk_table")
                .to("public", "no_pk_table2")
                .build());

        Exception exception = assertThrows(Exception.class, () -> {
            sourceStorage.start(targetStorage, configs, 1000, TransferMode.UPSERT);
        });

        Throwable rootCause = exception;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }

        assertTrue(rootCause.getMessage().contains("must have Primary Key or Unique Not Null Constraint"),
                "Ожидалась ошибка валидации PK, но получили: " + rootCause.getMessage());

        try (Connection conn = sourceDataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE public.no_pk_table");
        }
        try (Connection conn = targetDataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE public.no_pk_table2");
        }
    }
}
