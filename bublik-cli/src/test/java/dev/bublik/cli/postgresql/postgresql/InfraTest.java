package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.cli.TestResult;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static dev.bublik.cli.TestUtils.getJdbcProperties;
import static dev.bublik.cli.TestUtils.getResultCount;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class InfraTest {
    private static int rows = 10_000;
    private static JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/infraSource.sql");
    private static JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/infraTarget.sql");

    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(Collections.singletonList("5432:5432"));
        source.start();
        target.setPortBindings(Collections.singletonList("5433:5432"));
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

    @BeforeEach
    void clearTables() throws SQLException {
        String jdbcUrl = target.getJdbcUrl();
        String username = target.getUsername();
        String password = target.getPassword();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            PreparedStatement ps = connection.prepareStatement("truncate table public.t");
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void infraSourceFailure() throws Exception {
        CompletableFuture<TestResult> migrationTask = CompletableFuture.supplyAsync(() -> {
            try {
                return getResultCount(
                        "./postgresql/postgresql/yaml/infra.yaml",
                        "./postgresql/postgresql/json/infra.json",
                        rows,
                        false,
                        getJdbcProperties(source),
                        getJdbcProperties(target));
            } catch (Exception e) {
                throw new RuntimeException("Bublik migration thread failed unexpectedly", e);
            }
        });

        Thread.sleep(1_000);

        System.out.println("[TEST] CRITICAL: Stopping source database container mid-flight...");
        String containerId = source.getContainerId();
        source.getDockerClient().stopContainerCmd(containerId).exec();
        System.out.println("[TEST] Source container is now STOPPED.");

        System.out.println("[TEST] RECOVERY: Starting source database container back up...");
        source.getDockerClient().startContainerCmd(containerId).exec();
        System.out.println("[TEST] Source container is STARTING...");

        System.out.println("[TEST] Waiting for the background migration task to complete...");

        TestResult result;
        try {
            result = migrationTask.get(60, java.util.concurrent.TimeUnit.SECONDS);
            System.out.println("[TEST] Bublik finished its execution cycle.");
        } catch (java.util.concurrent.TimeoutException e) {
            migrationTask.cancel(true);
            throw new AssertionError("Test failed: Bublik hung or didn't finish within 60 seconds after recovery", e);
        }

        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());

        assertEquals(result.sourceCount(), result.targetCount(),
                "Несмотря на падение источника, Бублик должен восстановить упавшие чанки и перелить ровно 100% данных (At-Least-Once)");
    }

    @Test
    void infraTargetFailure() throws Exception {
        CompletableFuture<TestResult> migrationTask = CompletableFuture.supplyAsync(() -> {
            try {
                return getResultCount(
                        "./postgresql/postgresql/yaml/infra.yaml",
                        "./postgresql/postgresql/json/infra.json",
                        rows,
                        false,
                        getJdbcProperties(source),
                        getJdbcProperties(target));
            } catch (Exception e) {
                throw new RuntimeException("Bublik migration thread failed unexpectedly", e);
            }
        });

        Thread.sleep(1_000);

        System.out.println("[TEST] CRITICAL: Stopping target database container mid-flight...");
        String containerId = target.getContainerId();
        target.getDockerClient().stopContainerCmd(containerId).exec();
        System.out.println("[TEST] Target container is now STOPPED.");

        System.out.println("[TEST] RECOVERY: Starting target database container back up...");
        target.getDockerClient().startContainerCmd(containerId).exec();
        System.out.println("[TEST] Target container is STARTING...");

        System.out.println("[TEST] Waiting for the background migration task to complete...");

        TestResult result;
        try {
            result = migrationTask.get(60, java.util.concurrent.TimeUnit.SECONDS);
            System.out.println("[TEST] Bublik finished its execution cycle.");
        } catch (java.util.concurrent.TimeoutException e) {
            migrationTask.cancel(true);
            throw new AssertionError("Test failed: Bublik hung or didn't finish within 60 seconds after recovery", e);
        }

        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());

        assertEquals(result.sourceCount(), result.targetCount(),
                "Несмотря на падение источника, Бублик должен восстановить упавшие чанки и перелить ровно 100% данных (At-Least-Once)");
    }
}
