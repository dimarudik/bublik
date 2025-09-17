package org.bublik.cli;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.sql.SQLException;

import static org.bublik.cli.TestUtils.getResult;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PostgresToPostgresTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres:latest")
            .withDatabaseName("postgres")
//            .withCopyFileToContainer(MountableFile.forHostPath("images/bublik.png"), "/var/lib/postgresql/bublik.png")
            .withInitScript("pg2pg/sql/pg-init.sql");
    private static JdbcDatabaseContainer<?> destination = source;

    @BeforeAll
     static void setUp() throws SQLException {
        source.setPortBindings(java.util.Collections.singletonList("5432:5432"));
        source.start();
    }

    @Test
    void allTypes() throws IOException {
        TestResult result = getResult(
                "pg2pg/pg2pg.yaml",
                "pg2pg/cases/allTypes.json",
                rows,
                sync,
                source,
                destination);
        assertEquals(result.targetCount(), result.sourceCount());
    }

    @Test
    void targetTableNotExists() throws IOException {
        TestResult result = getResult(
                "pg2pg/pg2pg.yaml",
                "pg2pg/cases/targetTableNotExists.json",
                rows,
                sync,
                source,
                destination);
        assertEquals(result.targetCount(), result.sourceCount());
    }
}