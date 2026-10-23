package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.core.constants.TransferMode;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.Storage2Storage;
import dev.bublik.core.service.StorageService;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.postgres.storage.PostgresStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReplicationTest {
    private static final Network network = Network.newNetwork();

    private static JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("postgres")
            .withUsername("test")
            .withPassword("test")
            .withNetwork(network)
            .withNetworkAliases("source")
            .withCommand("postgres",
                    "-c", "wal_level=logical",
                    "-c", "max_wal_senders=10",
                    "-c", "max_replication_slots=10",
                    "-c", "ssl=off")
            .withInitScript("postgresql/postgresql/sql/replSource.sql");

    private static JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("postgres")
            .withUsername("test")
            .withPassword("test")
            .withNetwork(network)
            .withNetworkAliases("target")
            .withCommand("postgres",
                    "-c", "wal_level=logical",
                    "-c", "max_wal_senders=10",
                    "-c", "max_replication_slots=10",
                    "-c", "ssl=off")
            .withInitScript("postgresql/postgresql/sql/replTarget.sql")
            .dependsOn(source);


    @BeforeAll
    static void setUp() throws Exception {
        source.start();
        target.start();
    }

    @AfterAll
    static void clear() {
        source.stop();
        target.stop();
        network.close();
    }

    @Test
    void replicationTest() throws Exception {
        CountDownLatch latchBublikStarted = new CountDownLatch(1);
        CountDownLatch latchBublikFinished = new CountDownLatch(1);

        ExecutorService testExecutor = Executors.newFixedThreadPool(4);

        testExecutor.submit(() -> {
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test")) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO public.replicated_data (payload_text) VALUES ('Trx 1: Commit DURING migration')")) {
                    ps.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                latchBublikStarted.await();
                Thread.sleep(1_000);

                conn.commit();
                System.out.println("[CHAOS] ---> Transaction 1 COMMITTED DURIIIING Bublik work! <---");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        testExecutor.submit(() -> {
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test")) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO public.replicated_data (payload_text) VALUES ('Trx 2: Commit AFTER migration')")) {
                    ps.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                latchBublikFinished.await();

                conn.commit();
                System.out.println("[CHAOS] ---> Transaction 2 COMMITTED AFTER Bublik finished! <---");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread.sleep(500);

        Future<Void> workLoad = testExecutor.submit(() -> {
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test")) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO public.replicated_data (payload_text) VALUES (?)")) {
                    for (int i = 0; i < 500; i++) {
                        ps.setString(1, "Trx 1: " + i);
                        ps.executeUpdate();
                        if (i % 10 == 0) conn.commit();
                    }
                    conn.commit();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        Future<String> bublikFuture = testExecutor.submit(() -> {
            try {
                System.out.println("[BUBLIK-THREAD] Starting history migration engine...");

                latchBublikStarted.countDown();

                ConnectionProperty connectionProperty = new ConnectionProperty.Builder()
                        .threadCount(4)
                        .addFromProperty("url", source.getJdbcUrl().replace("?loggerLevel=OFF", ""))
                        .addFromProperty("user", source.getUsername())
                        .addFromProperty("password", source.getPassword())
                        .addFromProperty("snapshotIsolationEnabled", "true")
                        .addToProperty("url", target.getJdbcUrl().replace("?loggerLevel=OFF", "")
                                + "?options=-c%20session_replication_role=replica")
                        .addToProperty("user", target.getUsername())
                        .addToProperty("password", target.getPassword())
                        .build();
                List<Config> configs = new ArrayList<>();
                Config config1 = Config
                        .builder()
                        .from("public", "replicated_data")
                        .to("public", "replicated_data")
                        .build();
                configs.add(config1);
                Config config2 = Config
                        .builder()
                        .from("public", "a")
                        .to("public", "a")
                        .build();
                configs.add(config2);
                Storage2Storage storage2Storage = StorageService.init(connectionProperty, configs, 20_000, TransferMode.COPY);
                return storage2Storage.source().unwrap(JDBCStorage.class).unwrap(PostgresStorage.class).getTargetLsn();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        String capturedLsn = bublikFuture.get();
        System.out.println("[BUBLIK-THREAD] History migration finished successfully.");

        latchBublikFinished.countDown();
        workLoad.get();

        if (capturedLsn != null) {
            try (Connection sourceConn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test");
                 PreparedStatement ps = sourceConn.prepareStatement("SELECT pg_replication_slot_advance('bublik_slot', ?::pg_lsn)")) {
                ps.setString(1, capturedLsn);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        System.out.println("[TEST] Slot 'bublik_slot' successfully advanced to " + capturedLsn + " . Position verified.");
                    }
                }
            } catch (SQLException e) {
                System.out.println("[WARN] Slot might already be ahead: " + e.getMessage());
            }
        } else {
            System.out.println("[ERROR] Bublik failed to capture WAL LSN point!");
        }
        Thread.sleep(1000);

        System.out.println("[TEST] Activating logical subscription on target...");
        try (Connection targetConn = DriverManager.getConnection(target.getJdbcUrl(), "test", "test");
             Statement stmt = targetConn.createStatement()) {
            stmt.execute("ALTER SUBSCRIPTION bublik_sub ENABLE");
        }

        System.out.println("[TEST] Waiting for subscription synchronization...");
        Thread.sleep(5000);

        long sourceCount = 0;
        long targetCount = 0;

        try (Connection sourceConn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test");
             Statement stmt = sourceConn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM public.replicated_data")) {
            if (rs.next()) sourceCount = rs.getLong(1);
        }

        try (Connection targetConn = DriverManager.getConnection(target.getJdbcUrl(), "test", "test");
             Statement stmt = targetConn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM public.replicated_data")) {
            if (rs.next()) targetCount = rs.getLong(1);
        }

        System.out.println("[RESULT] SOURCE rows count: " + sourceCount);
        System.out.println("[RESULT] TARGET rows count: " + targetCount);

        assertEquals(sourceCount, targetCount, "Data counts do not match between Source and Target!");

        testExecutor.shutdown();
    }
}
