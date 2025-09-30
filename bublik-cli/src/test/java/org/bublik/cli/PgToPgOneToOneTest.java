package org.bublik.cli;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.*;

import static org.bublik.cli.TestUtils.getResult;
import static org.junit.jupiter.api.Assertions.assertEquals;

//@Disabled
class PgToPgOneToOneTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres:latest")
            .withDatabaseName("postgres")
//            .withCopyFileToContainer(MountableFile.forHostPath("images/bublik.png"), "/var/lib/postgresql/bublik.png")
            .withInitScript("./pg2pg/sql/pg-init.sql");
    private static JdbcDatabaseContainer<?> target = source;

    @BeforeAll
     static void setUp() throws SQLException {
        MountableFile mf = MountableFile.forClasspathResource("./images/bublik.png");
        source.addFileSystemBind(mf.getResolvedPath(), "/var/lib/postgresql/bublik.png", BindMode.READ_ONLY);
        source.setPortBindings(java.util.Collections.singletonList("5432:5432"));
        source.start();
    }

    @AfterAll
    static void clear() {
        source.stop();
    }

    @Test
    void allTypes() throws IOException {
        TestResult result = getResult(
                "./pg2pg/pg2pg.yaml",
                "./pg2pg/cases/allTypes.json",
                rows,
                sync,
                source,
                target);
        assertEquals(result.targetCount(), result.sourceCount());
    }

    @Test
    void targetTableNotExists() throws IOException {
        TestResult result = getResult(
                "./pg2pg/pg2pg.yaml",
                "./pg2pg/cases/targetTableNotExists.json",
                rows,
                sync,
                source,
                target);
        assertEquals(result.targetCount(), result.sourceCount());
    }

    @Test
    void notNullFailure() throws IOException {
        getResult(
                "./pg2pg/pg2pg.yaml",
                "./pg2pg/cases/notNullFailure.json",
                rows,
                sync,
                source,
                target);
        String jdbcUrl = source.getJdbcUrl();
        String username = source.getUsername();
        String password = source.getPassword();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            PreparedStatement ps = connection.prepareStatement("update public.not_null_failure set name = 'a' where id = 1000");
            ps.executeUpdate();
        }
        catch (SQLException e){
            throw new RuntimeException(e);
        }
        TestResult result = getResult(
                "./pg2pg/pg2pg.yaml",
                "./pg2pg/cases/notNullFailure.json",
                0,
                sync,
                source,
                target);
        assertEquals(result.targetCount(), result.sourceCount());
    }
}