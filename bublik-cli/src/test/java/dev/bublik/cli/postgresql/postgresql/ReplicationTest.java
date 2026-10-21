package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.core.constants.TransferMode;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.service.StorageService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
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
        // Защелки для синхронизации хаос-потоков
        CountDownLatch latchBublikStarted = new CountDownLatch(1);
        CountDownLatch latchBublikFinished = new CountDownLatch(1);

        // Пул на 3 потока: Бублик, Транзакция 1, Транзакция 2
        ExecutorService testExecutor = Executors.newFixedThreadPool(3);

        // =================================================================================
        // УСЛОВИЕ 1: Транзакция 1 (Коммит строго ВО ВРЕМЯ переливки)
        // =================================================================================
        testExecutor.submit(() -> {
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test")) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO public.replicated_data (payload_text) VALUES ('Trx 1: Commit DURING migration')")) {
                    ps.executeUpdate();
                }

                // Ждем, пока Бублик гарантированно начнет миграцию
                latchBublikStarted.await();

                // Имитируем паузу: даем Бублику поработать пару секунд и налить часть чанков
                Thread.sleep(1_000);

                conn.commit(); // Стреляем коммитом прямо во время миграции!
                System.out.println("[CHAOS] ---> Transaction 1 COMMITTED DURIIIING Bublik work! <---");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // =================================================================================
        // УСЛОВИЕ 2: Транзакция 2 (Коммит строго ПОСЛЕ переливки)
        // =================================================================================
        testExecutor.submit(() -> {
            try (Connection conn = DriverManager.getConnection(source.getJdbcUrl(), "test", "test")) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO public.replicated_data (payload_text) VALUES ('Trx 2: Commit AFTER migration')")) {
                    ps.executeUpdate();
                }

                // Ждем полного окончания работы Бублика
                latchBublikFinished.await();

                conn.commit(); // Заведомо после финиша Бублика
                System.out.println("[CHAOS] ---> Transaction 2 COMMITTED AFTER Bublik finished! <---");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Даем хаос-транзакциям 500мс, чтобы они успели открыться и сделать INSERT до Бублика
        Thread.sleep(500);

        // =================================================================================
        // ВЫНОС БУБЛИКА В ОТДЕЛЬНЫЙ ПОТОК
        // =================================================================================
        Future<Void> bublikFuture = testExecutor.submit(() -> {
            try {
                System.out.println("[BUBLIK-THREAD] Starting history migration engine...");

                // Сигнализируем Транзакции 1, что мы погнали (это произойдет прямо перед инициализацией)
                latchBublikStarted.countDown();

                ConnectionProperty connectionProperty = new ConnectionProperty.Builder()
                        .threadCount(4)
                        .addFromProperty("url", source.getJdbcUrl())
                        .addFromProperty("user", source.getUsername())
                        .addFromProperty("password", source.getPassword())
                        .addFromProperty("snapshotIsolationEnabled", "true")
                        .addToProperty("url", target.getJdbcUrl() + "?options=-c%20session_replication_role=replica")
                        .addToProperty("user", target.getUsername())
                        .addToProperty("password", target.getPassword())
                        .build();
                List<Config> configs = new ArrayList<>();
                Config config = Config
                        .builder()
                        .from("public", "replicated_data")
                        .to("public", "replicated_data")
                        .build();
                configs.add(config);
                StorageService.init(connectionProperty, configs, 20_000, TransferMode.COPY);
                return Void.TYPE.cast(null);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Блокируем main поток теста до полного завершения Бублика
        bublikFuture.get();
        System.out.println("[BUBLIK-THREAD] History migration finished successfully.");

        // Бублик завершил работу — отпускаем Транзакцию 2 на коммит
        latchBublikFinished.countDown();

        // Небольшой sleep, чтобы транзакция 2 успела физически закоммититься в БД
        Thread.sleep(1000);

        // =================================================================================
        // УСЛОВИЕ 3: Активируем логическую подписку на приемнике
        // =================================================================================
        System.out.println("[TEST] Activating logical subscription on target...");
        try (Connection targetConn = DriverManager.getConnection(target.getJdbcUrl(), "test", "test");
             Statement stmt = targetConn.createStatement()) {

            // Включаем подписку. Она начнет выгребать накопленный WAL из bublik_slot
            stmt.execute("ALTER SUBSCRIPTION bublik_sub ENABLE");
        }

        // Даем 5 секунд встроенному механизму Postgres, чтобы прочитать дельту
        System.out.println("[TEST] Waiting for subscription synchronization...");
        Thread.sleep(5000);

        // =================================================================================
        // УСЛОВИЕ 4: Проверяем сходимость данных (Data Consistency Check)
        // =================================================================================
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

        // Финальная проверка: исторический слой (100,000) + 2 наши контрольные строки = 100,002
        assertEquals(sourceCount, targetCount, "Data counts do not match between Source and Target!");

        testExecutor.shutdown();
    }
}
