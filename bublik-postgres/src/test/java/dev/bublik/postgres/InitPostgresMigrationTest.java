package dev.bublik.postgres;

import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.PseudoTable;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageService;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InitPostgresMigrationTest {
    static final JdbcDatabaseContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres"));

    @BeforeAll
    static void beforeAll() throws Exception {
        postgres.start();

        try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE source_users (id INT PRIMARY KEY, user_name VARCHAR(100))");
            stmt.execute("CREATE TABLE target_users (id INT PRIMARY KEY, user_name VARCHAR(100))");

            stmt.execute("INSERT INTO source_users (id, user_name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
        }
    }

    @AfterAll
    static void afterAll() {
        postgres.stop();
    }

    @Test
    void testPostgresToPostgresMigration() throws Exception {
        ConnectionProperty connectionProperty = getConnectionProperty();

        List<Config> configs = new ArrayList<>();
        Config tableConfig = new Config(
                "public",
                "source_users",
                "public",
                "target_users"
        );
        configs.add(tableConfig);

        Table chunkTable = new PseudoTable("public", "chunk");
        Table outboxTable = new PseudoTable("public", "outbox");

        StorageService.init(connectionProperty, configs, 1000, chunkTable, outboxTable);

        try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*), MIN(user_name) FROM target_users")) {

            assertTrue(rs.next());
            int count = rs.getInt(1);
            String firstUser = rs.getString(2);

            assertEquals(3, count, "Метод init должен был перенести ровно 3 записи");
            assertEquals("Alice", firstUser, "Данные внутри перенесенных строк должны совпадать");
        }
    }

    private static @NonNull ConnectionProperty getConnectionProperty() {
        Map<String, String> fromProps = new HashMap<>();
        fromProps.put("url", postgres.getJdbcUrl());
        fromProps.put("user", postgres.getUsername());
        fromProps.put("password", postgres.getPassword());

        Map<String, String> toProps = new HashMap<>();
        toProps.put("url", postgres.getJdbcUrl());
        toProps.put("user", postgres.getUsername());
        toProps.put("password", postgres.getPassword());

        return new ConnectionProperty(
                4,
                fromProps,
                toProps,
                new HashMap<>(),
                new HashMap<>()
        );
    }
}
