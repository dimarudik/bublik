package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.DummyTable;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageService;
import dev.bublik.postgres.model.PGTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static dev.bublik.cli.TestUtils.*;
import static dev.bublik.cli.TestUtils.chunkTable2;
import static dev.bublik.cli.TestUtils.getJdbcProperties;
import static dev.bublik.cli.TestUtils.getResult;
import static dev.bublik.cli.TestUtils.outboxTable;
import static dev.bublik.cli.TestUtils.outboxTable2;
import static dev.bublik.core.util.Utils.getStackTrace;
import static org.junit.jupiter.api.Assertions.*;

public class InfraTest {
    private static final JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/infraSource.sql");
    private static final JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/infraTarget.sql");

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

/*
    @Test
    void bigDataFailure() throws Exception {
        ConnectionProperty cp = getConnectionPropertyBigData();
        List<Config> cfgs = new ArrayList<>(Collections.singleton(
                Config.builder()
                        .from("public", "big")
                        .to("public", "big")
                        .build()
        ));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> {
            StorageService.init(cp, cfgs, 10_000, chunkTable, outboxTable);
        });
        assertTrue(ex.getMessage().contains("Ran out of memory"));
    }
*/

    @Disabled
    @Test
    void k8s() throws Exception {
        ConnectionProperty connectionProperty = getConnectionProperty(2);
        Table chkTable = new PGTable.Builder("public", "chunk_k8s").build();
        Table outxTable = new PGTable.Builder("public", "outbox_k8s").build();
        List<Config> configs = new ArrayList<>(Collections.singleton(
                Config.builder()
                        .from("public", "s")
                        .to("public", "k8s")
                        .build()
        ));
        ExecutorService service = Executors.newFixedThreadPool(4);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            futures.add(service.submit(() -> {
                try {
                    StorageService.init(connectionProperty, configs, 20_000, chkTable, outxTable);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            }));
        }
        for (Future<Boolean> future : futures) {
            try {
                boolean b = future.get();
                System.out.println("[TEST] Bublik finished its execution cycle.");
            } catch (Exception e) {
                assertTrue(getStackTrace(e).contains("violates not-null constraint"));
            }
        }

        int sourceCount = countRows(source, "select count(*) from public.s");
        int targetCount = countRows(target, "select count(*) from public.k8s");
        TestResult result = new TestResult(sourceCount, targetCount);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());

        amendNotNull(source, "update public.s set name = 'name' where id1 = 100000 and id2 = -100000");
        amendNotNull(source, "update public.s set name = 'name' where id1 = 200000 and id2 = -200000");
        amendNotNull(source, "update public.s set name = 'name' where id1 = 300000 and id2 = -300000");
        amendNotNull(source, "update public.s set name = 'name' where id1 = 400000 and id2 = -400000");

        for (int i = 0; i < 2; i++) {
            futures.add(service.submit(() -> {
                try {
                    StorageService.init(connectionProperty, configs, 0, chkTable, outxTable);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            }));
        }
        for (Future<Boolean> future : futures) {
            try {
                boolean b = future.get();
                System.out.println("[TEST] Bublik finished its execution cycle.");
            } catch (Exception e) {
                assertTrue(getStackTrace(e).contains("violates not-null constraint"));
            }
        }

        service.shutdown();
        service.close();

        sourceCount = countRows(source, "select count(*) from public.s");
        targetCount = countRows(target, "select count(*) from public.k8s");
        result = new TestResult(sourceCount, targetCount);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
//        Thread.sleep(900_000);
        assertEquals(result.sourceCount(), result.targetCount(),
                "Несмотря на падение источника, Бублик должен восстановить упавшие чанки и перелить ровно 100% данных (At-Least-Once)");
    }

    @Test
    void infraFailure() throws Exception {
        Table chkTable = new PGTable.Builder("public", "chunk").build();
        Table outxTable = new PGTable.Builder("public", "outbox").build();
        ConnectionProperty connectionProperty = getConnectionProperty(3);
        List<Config> configs = new ArrayList<>(Collections.singleton(
                Config.builder()
                        .from("public", "s")
                        .to("public", "t")
                        .build()
        ));

        CompletableFuture<Boolean> migrationTask = CompletableFuture.supplyAsync(() -> {
            try {
                StorageService.init(connectionProperty, configs, 30_000, chkTable, outxTable);
            } catch (Exception e) {
                throw new RuntimeException("Bublik migration thread failed unexpectedly", e);
            }
            return true;
        });

        Thread.sleep(500);
        source.execInContainer("psql", "-U", source.getUsername(), "-d", source.getDatabaseName(),
                "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + source.getUsername() + "' AND pid <> pg_backend_pid();");

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
//            Thread.sleep(900_000);
            assertEquals(result.sourceCount(), result.targetCount(),
                    "Несмотря на падение источника, Бублик должен восстановить упавшие чанки и перелить ровно 100% данных (At-Least-Once)");
        } catch (java.util.concurrent.TimeoutException e) {
            migrationTask.cancel(true);
            throw new AssertionError("Test failed: Bublik hung or didn't finish within 60 seconds after recovery", e);
        }

    }

    private ConnectionProperty getConnectionProperty(int threads) {
        Map<String, String> fromProps = new HashMap<>();
        fromProps.put("url", source.getJdbcUrl());
        fromProps.put("user", source.getUsername());
        fromProps.put("password", source.getPassword());
        fromProps.put("fetchSize", "1000");

        Map<String, String> toProps = new HashMap<>();
        toProps.put("url", target.getJdbcUrl());
        toProps.put("user", target.getUsername());
        toProps.put("password", target.getPassword());

        return new ConnectionProperty(
                threads,
                fromProps,
                toProps,
                new HashMap<>(),
                new HashMap<>()
        );
    }

    private ConnectionProperty getConnectionPropertyBigData() {
        Map<String, String> fromProps = new HashMap<>();
        fromProps.put("url", source.getJdbcUrl());
        fromProps.put("user", source.getUsername());
        fromProps.put("password", source.getPassword());
        fromProps.put("fetchSize", "10");

        Map<String, String> toProps = new HashMap<>();
        toProps.put("url", target.getJdbcUrl());
        toProps.put("user", target.getUsername());
        toProps.put("password", target.getPassword());

        return new ConnectionProperty(
                2,
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

    private void amendNotNull(JdbcDatabaseContainer<?> container, String sql) {
        String jdbcUrl = container.getJdbcUrl();
        String username = container.getUsername();
        String password = container.getPassword();

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
             Statement st = connection.createStatement()) {
            st.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
