package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageService;
import dev.bublik.postgres.model.PGTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static dev.bublik.core.util.Utils.getStackTrace;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InfraTest {
    private static final Network network = Network.newNetwork();

    private static final JdbcDatabaseContainer<?> master = new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("postgres")
            .withUsername("test")
            .withPassword("test")
            .withNetwork(network)
            .withNetworkAliases("master")
            .withCommand("postgres",
                    "-c", "wal_level=replica",
                    "-c", "max_wal_senders=10",
                    "-c", "max_replication_slots=10",
                    "-c", "ssl=off")
            .withInitScript("postgresql/postgresql/sql/infraSource.sql")
            .withCopyToContainer(
                    Transferable.of(
                            "#!/bin/bash\n" +
                                    "echo 'host replication test all trust' >> \"$PGDATA/pg_hba.conf\"\n" +
                                    "echo 'host all all all trust' >> \"$PGDATA/pg_hba.conf\"\n"
                    ),
                    "/docker-entrypoint-initdb.d/00_configure_replication.sh" // 👈 Кладем в папку хуков образа
            );
    private static final PostgreSQLContainer<?> replica = new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("postgres")
            .withUsername("test")
            .withPassword("test")
            .withNetwork(network)
            .withNetworkAliases("replica")
            .withCommand("bash", "-c",
                    "rm -rf /var/lib/postgresql/data/*; " +
                            "until pg_basebackup -h master -D /var/lib/postgresql/data -U test -vP -Fp -Xs -R; do echo 'Waiting for master...'; sleep 1; done; " +
                            "exec docker-entrypoint.sh postgres")
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept read-only connections.*\\s", 1))
            .dependsOn(master);
    private static final JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/infraTarget.sql");

    @BeforeAll
    static void setUp() throws Exception {
        master.start();
//        Thread.sleep(900_000);
        replica.start();
        target.start();
    }

    @AfterAll
    static void clear() {
        master.stop();
        replica.stop();
        target.stop();
        network.close();
    }

    @Test
    void k8sDatabasePromoteFailover() throws Exception {
        ConnectionProperty connectionProperty = getConnectionProperty(2);
        List<Config> configs = Collections.singletonList(
                Config.builder().from("public", "s").to("public", "k8s_failover").build()
        );

        ExecutorService service = Executors.newFixedThreadPool(4);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            futures.add(service.submit(() -> {
                try {
                    StorageService.init(connectionProperty, configs, 20_000);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            }));
        }

        Thread.sleep(400);

        System.out.println("[💥 DB CRASH] Stopping PostgreSQL Master Node immediately...");
        master.stop();

        System.out.println("[🚀 PROMOTE] Promoting Replica container to become the new READ-WRITE Master...");

        Container.ExecResult execResult = replica.execInContainer(
                "su", "-", "postgres", "-c", "/usr/lib/postgresql/17/bin/pg_ctl promote -D /var/lib/postgresql/data"
        );

        System.out.println("[🚀 PROMOTE] pg_ctl output: " + execResult.getStdout().trim());

        for (Future<Boolean> future : futures) {
            try {
                future.get();
                System.out.println("[TEST] Pod successfully survived Master Failover via Replica Promotion!");
            } catch (Exception e) {
                System.out.println("[⚠️ CRASH] Pod failed during failover tracking. Stacktrace:");
            }
        }

        service.shutdown();
        service.close();

        int sourceCount = countRows(connectionProperty.getFromProperty(), "select count(*) from public.s");
        int targetCount = countRows(connectionProperty.getToProperty(), "select count(*) from public.k8s_failover");

        System.out.println("Source count (New Master): " + sourceCount + ", Target count: " + targetCount);

//        Thread.sleep(900_000);
        assertEquals(sourceCount, targetCount,
                "Фейловер с промоушеном провалился! Бублик потерял чанки или не смог продолжить запись на новый мастер.");
    }

    @Test
    void k8s() throws Exception {
        ConnectionProperty connectionProperty = getConnectionProperty(2);
        System.out.println("SOURCE Connection property: " + connectionProperty.getFromProperties().get("url") + " " +
                connectionProperty.getFromProperties().get("user") + " " + connectionProperty.getFromProperties().get("password"));
        System.out.println("TARGET Connection property: " + connectionProperty.getToProperties().get("url") + " " +
                connectionProperty.getToProperties().get("user") + " " + connectionProperty.getToProperties().get("password"));
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
                    StorageService.init(connectionProperty, configs, 20_000);
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

        int sourceCount = countRows(connectionProperty.getFromProperty(), "select count(*) from public.s");
        int targetCount = countRows(connectionProperty.getToProperty(), "select count(*) from public.k8s");
        TestResult result = new TestResult(sourceCount, targetCount);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());

        amendNotNull(connectionProperty.getFromProperty(), "update public.s set name = 'name' where id1 = 100000 and id2 = -100000");
        amendNotNull(connectionProperty.getFromProperty(), "update public.s set name = 'name' where id1 = 200000 and id2 = -200000");
        amendNotNull(connectionProperty.getFromProperty(), "update public.s set name = 'name' where id1 = 300000 and id2 = -300000");
        amendNotNull(connectionProperty.getFromProperty(), "update public.s set name = 'name' where id1 = 400000 and id2 = -400000");

        for (int i = 0; i < 2; i++) {
            futures.add(service.submit(() -> {
                try {
                    StorageService.init(connectionProperty, configs, 0);
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

        sourceCount = countRows(connectionProperty.getFromProperty(), "select count(*) from public.s");
        targetCount = countRows(connectionProperty.getToProperty(), "select count(*) from public.k8s");
        result = new TestResult(sourceCount, targetCount);
        System.out.println("Source count: " + result.sourceCount() + ", target count: " + result.targetCount());
//        Thread.sleep(900_000);
        assertEquals(result.sourceCount(), result.targetCount(),
                "Несмотря на падение источника, Бублик должен восстановить упавшие чанки и перелить ровно 100% данных (At-Least-Once)");
    }

    @Test
    void killProcess() throws Exception {
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
        if (master.isRunning()) {
            master.execInContainer("psql", "-U", master.getUsername(), "-d", master.getDatabaseName(),
                    "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + master.getUsername() + "' AND pid <> pg_backend_pid();");
        } else if (replica.isRunning()) {
            replica.execInContainer("psql", "-U", replica.getUsername(), "-d", replica.getDatabaseName(),
                    "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + replica.getUsername() + "' AND pid <> pg_backend_pid();");
        }

        Thread.sleep(1_000);
        target.execInContainer("psql", "-U", target.getUsername(), "-d", target.getDatabaseName(),
                "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + target.getUsername() + "' AND pid <> pg_backend_pid();");

        try {
            boolean r = migrationTask.get(60, java.util.concurrent.TimeUnit.SECONDS);
            if (r) System.out.println("[TEST] Bublik finished its execution cycle.");
            int sourceCount = countRows(connectionProperty.getFromProperty(), "select count(*) from public.s");
            int targetCount = countRows(connectionProperty.getToProperty(), "select count(*) from public.t");
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
        String masterHostPort = "";
        if (master.isRunning()) {
            masterHostPort = master.getHost() + ":" + master.getMappedPort(5432) + ",";
        }
        String replicaHostPort = replica.getHost() + ":" + replica.getMappedPort(5432);
        String failoverUrl = "jdbc:postgresql://" + masterHostPort + replicaHostPort +
                "/postgres?targetServerType=primary";

        Map<String, String> fromProps = new HashMap<>();
        fromProps.put("url", failoverUrl);
        fromProps.put("user", master.getUsername());
        fromProps.put("password", master.getPassword());
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
        fromProps.put("url", master.getJdbcUrl());
        fromProps.put("user", master.getUsername());
        fromProps.put("password", master.getPassword());
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

    private int countRows(Properties properties, String sql) {
        String jdbcUrl = properties.getProperty("url");
        String username = properties.getProperty("user");
        String password = properties.getProperty("password");

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
             Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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

    private void amendNotNull(Properties properties, String sql) {
        String jdbcUrl = properties.getProperty("url");
        String username = properties.getProperty("user");
        String password = properties.getProperty("password");

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
             Statement st = connection.createStatement()) {
            st.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
}
