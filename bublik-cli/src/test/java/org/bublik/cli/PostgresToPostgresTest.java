package org.bublik.cli;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import org.bublik.core.model.ConnectionProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

class PostgresToPostgresTest {
    public static PostgreSQLContainer<?> source = new PostgreSQLContainer<>("postgres:latest")
        .withInitScript("pg-init-source.sql");

    public static PostgreSQLContainer<?> destination = new PostgreSQLContainer<>("postgres:latest")
        .withInitScript("pg-init-destination.sql");


    @BeforeAll
     static void setUp() throws SQLException {
        source.start();
        destination.start();
    }

    @Test
    void happyPassTest() {
        ConnectionProperty cp = BublikTestUtils.buildConnectionProperty(source, destination);
        Long countBeforeSynchronization = BublikTestUtils.countRows(destination, "public.test_table");
        assertEquals(0L, countBeforeSynchronization);

        App.runProcess(cp, BublikTestUtils.getFilePath("localtest.json"), 500, false);
        Long countAfterSynchronization = BublikTestUtils.countRows(destination, "public.test_table");
        assertEquals(10000L, countAfterSynchronization);
    }





}