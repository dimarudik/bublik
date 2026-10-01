package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.service.StorageService;
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

    private static final JdbcDatabaseContainer<?> srcMaster = new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("postgres")
            .withUsername("test")
            .withPassword("test")
            .withNetwork(network)
            .withNetworkAliases("src-master")
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
    private static final PostgreSQLContainer<?> srcReplica = new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("postgres")
            .withUsername("test")
            .withPassword("test")
            .withNetwork(network)
            .withNetworkAliases("src-replica")
            .withCommand("bash", "-c",
                    "rm -rf /var/lib/postgresql/data/*; " +
                            "until pg_basebackup -h src-master -D /var/lib/postgresql/data -U test -vP -Fp -Xs -R; do echo 'Waiting for master...'; sleep 1; done; " +
                            "exec docker-entrypoint.sh postgres")
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept read-only connections.*\\s", 1))
            .dependsOn(srcMaster);
    private static final JdbcDatabaseContainer<?> trgMaster = new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("postgres")
            .withUsername("test")
            .withPassword("test")
            .withNetwork(network)
            .withNetworkAliases("trg-master")
            .withCommand("postgres",
                    "-c", "wal_level=replica",
                    "-c", "max_wal_senders=10",
                    "-c", "max_replication_slots=10",
                    "-c", "ssl=off")
            .withInitScript("postgresql/postgresql/sql/infraTarget.sql")
            .withCopyToContainer(
                    Transferable.of(
                            "#!/bin/bash\n" +
                                    "echo 'host replication test all trust' >> \"$PGDATA/pg_hba.conf\"\n" +
                                    "echo 'host all all all trust' >> \"$PGDATA/pg_hba.conf\"\n"
                    ),
                    "/docker-entrypoint-initdb.d/00_configure_replication.sh" // 👈 Кладем в папку хуков образа
            );
    private static final PostgreSQLContainer<?> trgReplica = new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("postgres")
            .withUsername("test")
            .withPassword("test")
            .withNetwork(network)
            .withNetworkAliases("trg-replica")
            .withCommand("bash", "-c",
                    "rm -rf /var/lib/postgresql/data/*; " +
                            "until pg_basebackup -h trg-master -D /var/lib/postgresql/data -U test -vP -Fp -Xs -R; do echo 'Waiting for master...'; sleep 1; done; " +
                            "exec docker-entrypoint.sh postgres")
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept read-only connections.*\\s", 1))
            .dependsOn(trgMaster);


    @BeforeAll
    static void setUp() throws Exception {
        srcMaster.start();
        srcReplica.start();
        trgMaster.start();
        trgReplica.start();
    }

    @AfterAll
    static void clear() {
        srcMaster.stop();
        srcReplica.stop();
        trgMaster.stop();
        trgReplica.stop();
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

        System.out.println("[💥 DB CRASH] Stopping PostgreSQL Source Master Node immediately...");
        srcMaster.stop();
        System.out.println("[🚀 PROMOTE] Promoting Source Replica container to become the new READ-WRITE Master...");
        Container.ExecResult srcExecResult = srcReplica.execInContainer(
                "su", "-", "postgres", "-c", "/usr/lib/postgresql/17/bin/pg_ctl promote -D /var/lib/postgresql/data"
        );
        System.out.println("[🚀 PROMOTE] pg_ctl output: " + srcExecResult.getStdout().trim());

        Thread.sleep(800);

        System.out.println("[💥 DB CRASH] Stopping PostgreSQL Target Master Node immediately...");
        trgMaster.stop();
        System.out.println("[🚀 PROMOTE] Promoting Target Replica container to become the new READ-WRITE Master...");
        Container.ExecResult trgExecResult = trgReplica.execInContainer(
                "su", "-", "postgres", "-c", "/usr/lib/postgresql/17/bin/pg_ctl promote -D /var/lib/postgresql/data"
        );
        System.out.println("[🚀 PROMOTE] pg_ctl output: " + trgExecResult.getStdout().trim());

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
    void k8sNotNullFailure() throws Exception {
        ConnectionProperty connectionProperty = getConnectionProperty(2);
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
    void k8sKillPod() throws Exception {
        ConnectionProperty connectionProperty = getConnectionProperty(2);
        List<Config> configs = Collections.singletonList(
                Config.builder().from("public", "s").to("public", "k8s_killpod").build()
        );

        ExecutorService k8sCluster = Executors.newFixedThreadPool(4);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            final int podId = i + 1;
            futures.add(k8sCluster.submit(() -> {
                try {
                    StorageService.init(connectionProperty, configs, 5_000);
                } catch (Exception e) {
                    throw new RuntimeException("Healthy Pod-" + podId + " failed", e);
                }
                return true;
            }));
        }

        Future<Boolean> crashedPodFuture = k8sCluster.submit(() -> {
            try {
                StorageService.init(connectionProperty, configs, 5_000);
            } catch (Exception e) {
                throw new RuntimeException("Crashed Pod-4 thread execution interrupted", e);
            }
            return true;
        });
        futures.add(crashedPodFuture);

        Thread.sleep(100);

        crashedPodFuture.cancel(true);

        int survivedPodsFinished = 0;
        for (Future<Boolean> future : futures) {
            try {
                future.get();
                survivedPodsFinished++;
            } catch (Exception e) {
                System.out.println("[K8S] Intercepted expected terminal state of hard-killed Pod-4.");
            }
        }

        k8sCluster.shutdown();
        k8sCluster.close();

        int sourceCount = countRows(connectionProperty.getFromProperty(), "select count(*) from public.s");
        int targetCount = countRows(connectionProperty.getToProperty(), "select count(*) from public.k8s_killpod");

        System.out.println(">>> POD CRASH FAILOVER REPORT <<<");
        System.out.println("Surviving pods that completed successfully: " + survivedPodsFinished + "/3");
        System.out.println("Source count (Master): " + sourceCount + " | Target count (Target): " + targetCount);

        assertEquals(sourceCount, targetCount,
                "Фейловер пода провалился! Данные упавшего контейнера были безвозвратно потеряны.");
    }


    @Test
    void killProcess() throws Exception {
        ConnectionProperty connectionProperty = getConnectionProperty(3);
        List<Config> configs = new ArrayList<>(Collections.singleton(
                Config.builder()
                        .from("public", "s")
                        .to("public", "t")
                        .build()
        ));

        CompletableFuture<Boolean> migrationTask = CompletableFuture.supplyAsync(() -> {
            try {
                StorageService.init(connectionProperty, configs, 30_000);
            } catch (Exception e) {
                throw new RuntimeException("Bublik migration thread failed unexpectedly", e);
            }
            return true;
        });

        Thread.sleep(500);
        if (srcMaster.isRunning()) {
            srcMaster.execInContainer("psql", "-U", srcMaster.getUsername(), "-d", srcMaster.getDatabaseName(),
                    "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + srcMaster.getUsername() + "' AND pid <> pg_backend_pid();");
        } else if (srcReplica.isRunning()) {
            srcReplica.execInContainer("psql", "-U", srcReplica.getUsername(), "-d", srcReplica.getDatabaseName(),
                    "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + srcReplica.getUsername() + "' AND pid <> pg_backend_pid();");
        }

        Thread.sleep(1_000);
        if (trgMaster.isRunning()) {
            trgMaster.execInContainer("psql", "-U", trgMaster.getUsername(), "-d", trgMaster.getDatabaseName(),
                    "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + trgMaster.getUsername() + "' AND pid <> pg_backend_pid();");
        } else if (trgReplica.isRunning()) {
            trgReplica.execInContainer("psql", "-U", trgReplica.getUsername(), "-d", trgReplica.getDatabaseName(),
                    "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + trgReplica.getUsername() + "' AND pid <> pg_backend_pid();");
        }

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
        String fromHostPort = "";
        if (srcMaster.isRunning()) {
            fromHostPort = srcMaster.getHost() + ":" + srcMaster.getMappedPort(5432) + ",";
        }
        String fromReplica = srcReplica.getHost() + ":" + srcReplica.getMappedPort(5432);
        String fromUrl = "jdbc:postgresql://" + fromHostPort + fromReplica +
                "/postgres?targetServerType=primary";

        Map<String, String> fromProps = new HashMap<>();
        fromProps.put("url", fromUrl);
        fromProps.put("user", srcMaster.getUsername());
        fromProps.put("password", srcMaster.getPassword());
        fromProps.put("fetchSize", "1000");

        String toHostPort = "";
        if (trgMaster.isRunning()) {
            toHostPort = trgMaster.getHost() + ":" + trgMaster.getMappedPort(5432) + ",";
        }
        String toReplica = trgReplica.getHost() + ":" + trgReplica.getMappedPort(5432);
        String toUrl = "jdbc:postgresql://" + toHostPort + toReplica +
                "/postgres?targetServerType=primary";

        Map<String, String> toProps = new HashMap<>();
        toProps.put("url", toUrl);
        toProps.put("user", trgMaster.getUsername());
        toProps.put("password", trgMaster.getPassword());

        return new ConnectionProperty(
                threads,
                fromProps,
                toProps
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
