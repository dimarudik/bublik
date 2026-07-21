package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.PseudoTable;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InfraTest {
    private static final JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/infraSource.sql");
    private static final JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/infraTarget.sql");

    private final ConnectionProperty connectionProperty = getConnectionProperty();
    private final Table chunkTable = new PseudoTable("public", "chunk");
    private final Table outboxTable = new PseudoTable("public", "outbox");

    List<Config> configs = new ArrayList<>(Collections.singleton(
            Config.builder()
                    .from("public", "s")
                    .to("public", "t")
                    .build()
    ));

    @BeforeAll
    static void setUp() throws SQLException {
        source.start();
        target.start();
    }

    @AfterAll
    static void clear() {
        source.stop();
        target.stop();
    }

    @Test
    void infraFailure() throws Exception {

        CompletableFuture<Boolean> migrationTask = CompletableFuture.supplyAsync(() -> {
            try {
                StorageService.init(connectionProperty, configs, 30_000, chunkTable, outboxTable);
            } catch (Exception e) {
                throw new RuntimeException("Bublik migration thread failed unexpectedly", e);
            }
            return true;
        });

        Thread.sleep(500);
        source.execInContainer("psql", "-U", target.getUsername(), "-d", target.getDatabaseName(),
                "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + target.getUsername() + "' AND pid <> pg_backend_pid();");

        Thread.sleep(1_000);
        target.execInContainer("psql", "-U", target.getUsername(), "-d", target.getDatabaseName(),
                "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + target.getUsername() + "' AND pid <> pg_backend_pid();");

        try {
            boolean r = migrationTask.get(60, java.util.concurrent.TimeUnit.SECONDS);
            if (r) System.out.println("[TEST] Bublik finished its execution cycle.");
            int sourceCount = countRows(source, "select count(*) from public.s");
            int targetCount = countRows(target, "select count(*) from public.t");
            TestResult result = new TestResult(sourceCount, targetCount);
            System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());

            assertEquals(result.sourceCount(), result.targetCount(),
                    "Несмотря на падение источника, Бублик должен восстановить упавшие чанки и перелить ровно 100% данных (At-Least-Once)");
        } catch (java.util.concurrent.TimeoutException e) {
            migrationTask.cancel(true);
            throw new AssertionError("Test failed: Bublik hung or didn't finish within 60 seconds after recovery", e);
        }

    }

    private ConnectionProperty getConnectionProperty() {
        Map<String, String> fromProps = new HashMap<>();
        fromProps.put("url", source.getJdbcUrl());
        fromProps.put("user", source.getUsername());
        fromProps.put("password", source.getPassword());

        Map<String, String> toProps = new HashMap<>();
        toProps.put("url", target.getJdbcUrl());
        toProps.put("user", target.getUsername());
        toProps.put("password", target.getPassword());

        return new ConnectionProperty(
                24,
                fromProps,
                toProps,
                new HashMap<>(),
                new HashMap<>()
        );
    }

    private int countRows(JdbcDatabaseContainer<?> container, String sql) {
        String jdbcUrl = container.getJdbcUrl();
        String username = container.getUsername();
        String password = container.getPassword();

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
             Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
