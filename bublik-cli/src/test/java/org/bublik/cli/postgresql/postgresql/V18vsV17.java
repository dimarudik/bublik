package org.bublik.cli.postgresql.postgresql;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.SQLException;
import java.util.Collections;

public class V18vsV17 {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source1 = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgresql")
            .withInitScript("./pg2pg/sql/manyToOneSource1.sql");
    private static JdbcDatabaseContainer<?> target1 = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgresql")
            .withInitScript("./pg2pg/sql/manyToOneTarget.sql");
    private static JdbcDatabaseContainer<?> source2 = new PostgreSQLContainer<>("postgres:17.6")
            .withDatabaseName("postgresql")
            .withInitScript("./pg2pg/sql/manyToOneSource2.sql");
    private static JdbcDatabaseContainer<?> target2 = new PostgreSQLContainer<>("postgres:17.6")
            .withDatabaseName("postgresql")
            .withInitScript("./pg2pg/sql/manyToOneTarget.sql");

    @BeforeAll
    static void setUp() throws SQLException {
        source1.setPortBindings(Collections.singletonList("5432:5432"));
        source1.start();
        target1.setPortBindings(Collections.singletonList("5433:5432"));
        target1.start();
        source2.setPortBindings(Collections.singletonList("5434:5432"));
        source2.start();
        target2.setPortBindings(Collections.singletonList("5435:5432"));
        target2.start();
    }

    @AfterAll
    static void clear() {
        source1.stop();
        target1.stop();
        source2.stop();
        target2.stop();
        while (source1.isRunning() || target1.isRunning() || source2.isRunning() || target2.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
