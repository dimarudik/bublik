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
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReplicationTest {
    private static final System.Logger log = System.getLogger(ReplicationTest.class.getName());
    private static final Network network = Network.newNetwork();
    private final String slotName = "bublik_slot";

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

    // psql "postgresql://test:test@localhost:53881/postgres?replication=database"
//    @Test
    @RepeatedTest(10)
    void replicationTest() throws Exception {
        try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword());
             Statement stmt1 = conn.createStatement()) {
            stmt1.execute("CREATE PUBLICATION bublik_pub FOR TABLE public.replicated_data");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try (Connection conn = DriverManager.getConnection(target.getJdbcUrl(), target.getUsername(), target.getPassword());
             Statement stmt1 = conn.createStatement()) {
            stmt1.execute(
        """
                CREATE SUBSCRIPTION bublik_sub
                CONNECTION 'host=source port=5432 dbname=postgres user=test password=test'
                PUBLICATION bublik_pub
                WITH (enabled = false, create_slot = false, copy_data = false, slot_name = '%slot_name')
            """.replace("%slot_name", slotName));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        CountDownLatch latchBublikStarted = new CountDownLatch(1);
        ExecutorService testExecutor = Executors.newFixedThreadPool(10);

        testExecutor.submit(() -> {
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test")) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO public.replicated_data (id, payload_text) VALUES (1000001,'Trx 1: Commit DURING migration')")) {
                    ps.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                latchBublikStarted.await();
                Thread.sleep(2_000);

                conn.commit();
                log.log(System.Logger.Level.INFO,"[CHAOS] ---> Transaction 1 COMMITTED DURIIIING Bublik work! <---");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Future<Long> wl1 = testExecutor.submit(() -> {
            long l = 0;
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test")) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO public.replicated_data (id, payload_text) VALUES (?, ?)")) {
                    for (int i = 1000002; i < 1002000; i++) {
                        ps.setLong(1, i);
                        ps.setString(2, "Trx 1: " + i);
                        l += ps.executeUpdate();
                        if (i % 10 == 0) conn.commit();
                    }
                    conn.commit();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return l;
        });

        Future<Long> wl2 = testExecutor.submit(() -> {
            long l = 0;
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test")) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO public.replicated_data (id, payload_text) VALUES (?, ?)")) {
                    for (int i = 1100002; i < 1102000; i++) {
                        ps.setLong(1, i);
                        ps.setString(2, "Trx 1: " + i);
                        l += ps.executeUpdate();
                        if (i % 10 == 0) conn.commit();
                    }
                    conn.commit();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return l;
        });

        Future<Long> wl3 = testExecutor.submit(() -> {
            long updateCount = 0, deleteCount = 0;
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test")) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "update public.replicated_data set payload_text = 'Trx new...' where id between ? and ?");
                     PreparedStatement ps2 = conn.prepareStatement("delete from public.replicated_data where id = ?")) {
                    Random random = new Random();
                    for (int i = 0; i < 3000; i++) {
                        int r = random.nextInt(1, 500_000);
                        ps.setInt(1, r);
                        ps.setInt(2, r + 49);
                        updateCount += ps.executeUpdate();
                        int r2 = random.nextInt(1, 1000_000);
                        ps2.setInt(1, r2);
                        deleteCount += ps2.executeUpdate();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return updateCount;
        });

        Future<Long> wl4 = testExecutor.submit(() -> {
            long updateCount = 0;
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test")) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "update public.replicated_data set payload_text = 'Trx new...' where id between ? and ?")) {
                    Random random = new Random();
                    for (int i = 0; i < 3000; i++) {
                        int r = random.nextInt(500_000, 900_000);
                        ps.setInt(1, r);
                        ps.setInt(2, r + 49);
                        updateCount += ps.executeUpdate();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return updateCount;
        });

        Future<String> bublikFuture = testExecutor.submit(() -> {
            try {
                log.log(System.Logger.Level.INFO,"[BUBLIK-THREAD] Starting history migration engine...");
                latchBublikStarted.countDown();

                ConnectionProperty connectionProperty = new ConnectionProperty.Builder()
                        .threadCount(4)
                        .addFromProperty("url", source.getJdbcUrl().replace("?loggerLevel=OFF", ""))
                        .addFromProperty("user", source.getUsername())
                        .addFromProperty("password", source.getPassword())
                        .addFromProperty("snapshotIsolationEnabled", "true")
                        .addFromProperty("slotName", slotName)
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
        log.log(System.Logger.Level.INFO,"[BUBLIK-THREAD] History migration finished successfully.");

        Long res1 = wl1.get();
        System.out.println("(WL1) Inserted rows: " + res1);
        Long res2 = wl2.get();
        System.out.println("(WL2) Inserted rows: " + res2);
        Long res3 = wl3.get();
        System.out.println("(WL3) Updated rows: " + res3);
        Long res4 = wl4.get();
        System.out.println("(WL4) Updated rows: " + res4);

        log.log(System.Logger.Level.INFO,"[TEST] Activating logical subscription on target...");
        try (Connection targetConn = DriverManager.getConnection(target.getJdbcUrl(), "test", "test");
             Statement stmt = targetConn.createStatement()) {
            stmt.execute("ALTER SUBSCRIPTION bublik_sub ENABLE");
        }

        String validationSql = "SELECT COUNT(*), sum(hashtext(payload_text)::bigint + hashtext(reverse(payload_text))::bigint) as name FROM public.replicated_data";
        long sourceCount = 0, sPayload = 0;
        try (Connection sourceConn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test");
             Statement stmt = sourceConn.createStatement();
             ResultSet rs = stmt.executeQuery(validationSql)) {
            if (rs.next()) {
                sourceCount = rs.getLong(1);
                sPayload = rs.getLong(2);
            }
        }

        long targetCount = 0, tPayload = 0;
        int tries = 20;
        while (sourceCount != targetCount || sPayload != tPayload) {
            String restartLsn = "", confirmedFlushLsn = "";
            try (Connection targetConn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test");
                 Statement stmt2 = targetConn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery(
                         "select restart_lsn, confirmed_flush_lsn lag from pg_replication_slots where slot_name = '%slot_name'"
                                 .replace("%slot_name", slotName))) {
                if (rs2.next()) {
                    restartLsn = rs2.getString(1);
                    confirmedFlushLsn = rs2.getString(2);
                }
            }
            try (Connection targetConn = DriverManager.getConnection(target.getJdbcUrl(), "test", "test");
                 Statement stmt = targetConn.createStatement();
                 ResultSet rs = stmt.executeQuery(validationSql)) {
                if (rs.next()) {
                    targetCount = rs.getLong(1);
                    tPayload = rs.getLong(2);
                }
            }
            Thread.sleep(500);
            tries--;
            log.log(System.Logger.Level.INFO,"[TEST] {0} tries left. The diff is: {1} {2}, {3} {4}}. restart_lsn: {5} confirmed_flush_lsn: {6}",
                    tries, sourceCount, targetCount, sPayload, tPayload, restartLsn, confirmedFlushLsn);
            if (tries == 0) break;
        }

        log.log(System.Logger.Level.INFO,"[RESULT] SOURCE: {0} {1}", sourceCount, sPayload);
        log.log(System.Logger.Level.INFO,"[RESULT] TARGET: {0} {1}", targetCount, tPayload);

        System.err.println("=== SOURCE CONTAINER LOGS ==================================================================");
        System.err.println(source.getLogs());
        System.err.println("=== TARGET CONTAINER LOGS ==================================================================");
        System.err.println(target.getLogs());

//        if (sPayload != tPayload) Thread.sleep(900_000);

        assertEquals(sourceCount, targetCount, "Data counts do not match between Source and Target!");
        assertEquals(sPayload, tPayload, "HASH of payload do not match between Source and Target!");
        testExecutor.shutdown();

        try (Connection conn = DriverManager.getConnection(target.getJdbcUrl(), target.getUsername(), target.getPassword());
             Statement stmt = conn.createStatement();
             Statement stmt1 = conn.createStatement();
             Statement stmt2 = conn.createStatement();
             Statement stmt3 = conn.createStatement()) {
            stmt.execute("alter subscription bublik_sub disable");
            stmt2.execute("drop subscription bublik_sub");
            stmt1.execute("truncate table public.replicated_data cascade");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword());
             Statement stmt1 = conn.createStatement();
             Statement stmt2 = conn.createStatement();
             Statement stmt3 = conn.createStatement();
             Statement stmt4 = conn.createStatement()) {
//            stmt1.execute("select pg_drop_replication_slot('bublik_slot')");
            stmt2.execute("delete from public.replicated_data where id > 1000000");
            stmt3.execute("vacuum public.replicated_data");
            stmt4.execute("drop publication bublik_pub");
        } catch (SQLException e) {
            e.printStackTrace();
        }

//        Thread.sleep(900_000);
    }

    /*
        if (capturedLsn != null) {
            try (Connection sourceConn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test");
                 PreparedStatement ps = sourceConn.prepareStatement("SELECT pg_replication_slot_advance('bublik_slot', ?::pg_lsn)")) {
                ps.setString(1, capturedLsn);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        log.log(System.Logger.Level.INFO,
                                "[TEST] Slot 'bublik_slot' successfully advanced to {0}", capturedLsn);
                    }
                }
            } catch (SQLException e) {
                System.out.println("[WARN] Slot might already be ahead: " + e.getMessage());
            }
        } else {
            System.out.println("[ERROR] Bublik failed to capture WAL LSN point!");
        }
        Thread.sleep(1000);
*/

}
