package dev.bublik.cli.postgresql.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.service.StorageService;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.*;
import java.util.List;

import static dev.bublik.cli.App.getConfigs;
import static dev.bublik.cli.TestUtils.*;
import static dev.bublik.cli.addons.Utils.connectionProperty;
import static org.junit.jupiter.api.Assertions.*;

public class PostgresToPostgresTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./postgresql/postgresql/sql/pg-init.sql");
    private static JdbcDatabaseContainer<?> target = source;

    @BeforeAll
     static void setUp() throws SQLException {
        MountableFile mf = MountableFile.forClasspathResource("./images/bublik.png");
        source.addFileSystemBind(mf.getResolvedPath(), "/var/lib/postgresql/bublik.png", BindMode.READ_ONLY);
        source.setPortBindings(java.util.Collections.singletonList("5432:5432"));
        source.start();
        while (!source.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @AfterAll
    static void clear() {
        source.stop();
        while (source.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void allTypes() throws IOException, InterruptedException {
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "./postgresql/postgresql/json/allTypes.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        TestResult result2 = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "./postgresql/postgresql/json/allTypes.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
//        Thread.sleep(60_000);
        assertEquals(result.sourceCount(), result2.targetCount() - result.targetCount());
    }

    @Test
    void isNotPartitioned() throws IOException, InterruptedException, SQLException {
        ConnectionProperty property = connectionProperty(
                PostgresToPostgresTest.class.getResourceAsStream("/postgresql/postgresql/yaml/pg2pg.yaml"));
        List<Config> configs = getConfigs(
                PostgresToPostgresTest.class.getResourceAsStream("/postgresql/postgresql/json/isNotPartitioned.json"));

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                StorageService.init(property, configs, false, 50_000, "_bublik"));
        assertTrue(ex.getMessage().contains("Partitioned tables are not supported"));
    }

    @Test
    void targetTableNotExists() throws IOException, InterruptedException {
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "./postgresql/postgresql/json/targetTableNotExists.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    void notNullFailure() throws IOException, InterruptedException {
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "postgresql/postgresql/json/notNullFailure.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));

        if (result.sourceCount() != result.targetCount()) {
            String jdbcUrl = source.getJdbcUrl();
            String username = source.getUsername();
            String password = source.getPassword();
            try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
                PreparedStatement ps = connection.prepareStatement("update public.not_null_failure set name = 'a' where id = 1000");
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        TestResult result2 = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "postgresql/postgresql/json/notNullFailure.json",
                0,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        assertEquals(result2.sourceCount(), result2.targetCount());
    }

    @Test
    void serialColumn() throws IOException, InterruptedException {
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "postgresql/postgresql/json/serialColumn.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    void emptySourceTable() throws IOException, InterruptedException {
        TestResult result = getResultCount(
                "./postgresql/postgresql/yaml/pg2pg.yaml",
                "postgresql/postgresql/json/emptySourceTable.json",
                rows,
                sync,
                getJdbcProperties(source),
                getJdbcProperties(target));
        System.out.println("source count: " + result.sourceCount());
        System.out.println("target count: " + result.targetCount());
        assertEquals(result.sourceCount(), result.targetCount());
    }
}