package org.bublik.cli;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.bublik.cli.TestUtils.getJdbcProperties;
import static org.bublik.cli.TestUtils.getResult;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PgToPgOneToManyTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./pg2pg/sql/oneToManySource.sql");
    private static JdbcDatabaseContainer<?> target1 = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./pg2pg/sql/oneToManyTarget1.sql");
    private static JdbcDatabaseContainer<?> target2 = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./pg2pg/sql/oneToManyTarget2.sql");

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

//    @Test
    void OneToMany() throws IOException {
        ExecutorService service = Executors.newFixedThreadPool(2);
        List<Future<TestResult>> futures = new ArrayList<>();
        long targetCount = 0;
        long sourceCount = 0;

        futures.add(service.submit(() -> getResult(
                "./pg2pg/manyToOneSource1.yaml",
                "pg2pg/mappings/manyToOneSource1.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target1))
        ));

        futures.add(service.submit(() -> getResult(
                "./pg2pg/manyToOneSource2.yaml",
                "pg2pg/mappings/manyToOneSource2.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target2))
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
        assertEquals(targetCount, sourceCount);
    }
}
