package org.bublik.cli;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
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

import static org.bublik.cli.TestUtils.getResult;
import static org.junit.jupiter.api.Assertions.assertEquals;

//@Disabled
public class PgToPgManyToOneTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source1 = new PostgreSQLContainer<>("postgres:latest")
            .withDatabaseName("postgres")
            .withInitScript("./pg2pg/sql/manyToOneSource1.sql");
    private static JdbcDatabaseContainer<?> source2 = new PostgreSQLContainer<>("postgres:latest")
            .withDatabaseName("postgres")
            .withInitScript("./pg2pg/sql/manyToOneSource2.sql");
    private static JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres:latest")
            .withDatabaseName("postgres")
            .withInitScript("./pg2pg/sql/manyToOneTarget.sql");

    @BeforeAll
    static void setUp() throws SQLException {
        source1.setPortBindings(Collections.singletonList("5432:5432"));
        source1.start();
        source2.setPortBindings(Collections.singletonList("5433:5432"));
        source2.start();
        target.setPortBindings(Collections.singletonList("5434:5432"));
        target.start();
    }

    @AfterAll
    static void clear() {
        source1.stop();
        source2.stop();
        target.stop();
    }

    @Test
    void ManyToOne() throws IOException {
        ExecutorService service = Executors.newFixedThreadPool(2);
        List<Future<TestResult>> futures = new ArrayList<>();
        long targetCount = 0;
        long sourceCount = 0;

        futures.add(service.submit(() -> getResult(
                "./pg2pg/manyToOneSource1.yaml",
                "pg2pg/cases/manyToOneSource1.json",
                rows,
                sync,
                source1,
                target)));

        futures.add(service.submit(() -> getResult(
                "./pg2pg/manyToOneSource2.yaml",
                "pg2pg/cases/manyToOneSource2.json",
                rows,
                sync,
                source2,
                target)));

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
