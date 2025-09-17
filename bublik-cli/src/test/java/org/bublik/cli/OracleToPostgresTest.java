package org.bublik.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class OracleToPostgresTest {
/*
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("ora2pg/sql/00_init.sql");
    private static JdbcDatabaseContainer<?> destination = new PostgreSQLContainer<>("postgres:latest")
            .withDatabaseName("postgres")
            .withCopyFileToContainer(MountableFile.forHostPath("images/bublik.png"), "/var/lib/postgresql/bublik.png")
            .withInitScript("pg2pg/sql/pg-init.sql");

//    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(java.util.Collections.singletonList("1521:1521"));
        source.start();
        destination.setPortBindings(java.util.Collections.singletonList("5432:5432"));
        destination.start();
    }

//    @Test
    void parted() throws IOException {
        TestResult result = getResult(
                "ora2pg/ora2pg.yaml",
                "ora2pg/cases/parted.json",
                rows,
                sync,
                source,
                destination);
        assertEquals(result.targetCount(), result.sourceCount());
    }

//    @Test
    void leftJoin() throws IOException {
        TestResult result = getResult(
                "ora2pg/ora2pg.yaml",
                "ora2pg/cases/leftJoin.json",
                rows,
                sync,
                source,
                destination);
        assertEquals(result.targetCount(), result.sourceCount());
    }

//    @Test
    void columnFromMany() throws IOException {
        TestResult result = getResult(
                "ora2pg/ora2pg.yaml",
                "ora2pg/cases/columnFromMany.json",
                rows,
                sync,
                source,
                destination);
        assertEquals(result.targetCount(), result.sourceCount());
    }
*/
}
