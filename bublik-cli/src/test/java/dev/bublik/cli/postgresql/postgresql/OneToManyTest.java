package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.cli.TestResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static dev.bublik.cli.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

//@Disabled
public class OneToManyTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/oneToManySource.sql");
    private static JdbcDatabaseContainer<?> target1 = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/oneToManyTarget1.sql");
    private static JdbcDatabaseContainer<?> target2 = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/oneToManyTarget2.sql");

    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(Collections.singletonList("5432:5432"));
        source.start();
        target1.setPortBindings(Collections.singletonList("5433:5432"));
        target1.start();
        target2.setPortBindings(Collections.singletonList("5434:5432"));
        target2.start();
    }

    @AfterAll
    static void clear() {
        source.stop();
        target1.stop();
        target2.stop();
        while (source.isRunning() || target1.isRunning() || target2.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void oneToMany() throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(2);
        List<Future<TestResult>> futures = new ArrayList<>();
        long targetCount = 0;
        long sourceCount = 0;

        futures.add(service.submit(() -> getResultCount(
                "postgresql/postgresql/yaml/oneToManyTarget1.yaml",
                "postgresql/postgresql/json/oneToManyTarget.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target1),
                chunkTable,
                outboxTable)
        ));

        futures.add(service.submit(() -> getResultCount(
                "postgresql/postgresql/yaml/oneToManyTarget2.yaml",
                "postgresql/postgresql/json/oneToManyTarget.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target2),
                chunkTable2,
                outboxTable2)
        ));

        for (Future<?> future : futures) {
            try {
                TestResult c = (TestResult) future.get();
                targetCount += c.targetCount();
                sourceCount += c.sourceCount();
                Thread.sleep(2);
            } catch (Exception e) {
                service.shutdownNow();
                throw new RuntimeException(e);
            }
        }

        service.shutdown();
        service.close();
        System.out.println("sourceCount = " + sourceCount + "\ntargetCount = " + targetCount);
        assertEquals(sourceCount, targetCount);
    }

    @Test
    void notNullFailure2() throws Exception {
        long targetCount = 0;
        long sourceCount = 0;

        ExecutorService service = Executors.newFixedThreadPool(2);
        List<Future<TestResult>> futures = new ArrayList<>();
        futures.add(service.submit(() -> getResultCount(
                "postgresql/postgresql/yaml/oneToManyTarget1.yaml",
                "postgresql/postgresql/json/notNullFailure2.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target1),
                chunkTable,
                outboxTable)
        ));
        futures.add(service.submit(() -> getResultCount(
                            "postgresql/postgresql/yaml/oneToManyTarget2.yaml",
                            "postgresql/postgresql/json/notNullFailure2.json",
                            rows,
                            sync,
                            getJdbcProperties(source),
                            getJdbcProperties(target2),
                            chunkTable2,
                            outboxTable2)
        ));

        for (Future<?> future : futures) {
            try {
                TestResult c = (TestResult) future.get();
                targetCount += c.targetCount();
                sourceCount += c.sourceCount();
                Thread.sleep(2);
            } catch (Exception e) {
//                assertTrue(e.getMessage().contains("Ending write to copy failed"));
                service.shutdownNow();
            }
        }
        service.shutdown();
        service.close();

        targetCount = 0;
        sourceCount = 0;
        String jdbcUrl = source.getJdbcUrl();
        String username = source.getUsername();
        String password = source.getPassword();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            PreparedStatement ps = connection.prepareStatement("update public.not_null_failure set name = 'a' where id = 1000");
            ps.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        System.out.println("sourceCount = " + sourceCount + "\ntargetCount = " + targetCount);

        ExecutorService service2 = Executors.newFixedThreadPool(2);
        List<Future<TestResult>> futures2 = new ArrayList<>();
        futures2.add(service2.submit(() -> getResultCount(
                "postgresql/postgresql/yaml/oneToManyTarget1.yaml",
                "postgresql/postgresql/json/notNullFailure2.json",
                0,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target1),
                chunkTable,
                outboxTable)
        ));
        futures2.add(service2.submit(() -> getResultCount(
                "postgresql/postgresql/yaml/oneToManyTarget2.yaml",
                "postgresql/postgresql/json/notNullFailure2.json",
                0,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target2),
                chunkTable2,
                outboxTable2)
        ));

        for (Future<?> future : futures2) {
            try {
                TestResult c = (TestResult) future.get();
                targetCount += c.targetCount();
                sourceCount += c.sourceCount();
                Thread.sleep(2);
            } catch (Exception e) {
                service2.shutdownNow();
                throw new RuntimeException(e);
            }
        }
        service2.shutdown();
        service2.close();
        System.out.println("sourceCount = " + sourceCount + "\ntargetCount = " + targetCount);
//        Thread.sleep(60_000);
        assertEquals(sourceCount, targetCount);
    }
}
