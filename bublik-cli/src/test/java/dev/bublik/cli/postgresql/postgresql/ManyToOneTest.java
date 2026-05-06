package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.cli.TestResult;
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

import static dev.bublik.cli.TestUtils.getJdbcProperties;
import static dev.bublik.cli.TestUtils.getResultCount;
import static org.junit.jupiter.api.Assertions.assertEquals;

//@Disabled
public class ManyToOneTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source1 = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/manyToOneSource1.sql");
    private static JdbcDatabaseContainer<?> source2 = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/manyToOneSource2.sql");
    private static JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("postgresql/postgresql/sql/manyToOneTarget.sql");

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
        while (source1.isRunning() || source2.isRunning() || target.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void ManyToOne() throws IOException {
        ExecutorService service = Executors.newFixedThreadPool(2);
        List<Future<TestResult>> futures = new ArrayList<>();
        long targetCount = 0;
        long sourceCount = 0;

        futures.add(service.submit(() -> getResultCount(
                "postgresql/postgresql/yaml/manyToOneSource1.yaml",
                "postgresql/postgresql/json/manyToOneSource1.json",
                rows,
                sync,
                getJdbcProperties(source1),
                getJdbcProperties(target),
                "_bublik_chunk_01")
        ));

        futures.add(service.submit(() -> getResultCount(
                "postgresql/postgresql/yaml/manyToOneSource2.yaml",
                "postgresql/postgresql/json/manyToOneSource2.json",
                rows,
                sync,
                getJdbcProperties(source2),
                getJdbcProperties(target),
                "_bublik_chunk_02")
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
        assertEquals(sourceCount, targetCount);
    }
}
