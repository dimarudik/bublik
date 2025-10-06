package org.bublik.cli.oracle.ydb;

import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.oracle.OracleContainer;

import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class OracleToYdbTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("ora2pg/sql/00_init.sql");
//    private static YdbDockerContainer target = new YdbDockerContainer(new YdbEnvironment(), new PortsGenerator());

    void setUp() throws IOException {
//        target.init();
        assertEquals(0, 0);
    }
}
