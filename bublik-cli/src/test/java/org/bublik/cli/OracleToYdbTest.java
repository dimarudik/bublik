package org.bublik.cli;

import org.bublik.cli.ydb.PortsGenerator;
import org.bublik.cli.ydb.YdbDockerContainer;
import org.bublik.cli.ydb.YdbEnvironment;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.oracle.OracleContainer;

import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OracleToYdbTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("ora2pg/sql/00_init.sql");
    private static YdbDockerContainer target = new YdbDockerContainer(new YdbEnvironment(), new PortsGenerator());

    void setUp() throws IOException {
        target.init();
        assertEquals(0, 0);
    }
}
