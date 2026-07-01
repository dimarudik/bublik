package dev.bublik.cli.mssql.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.cli.TestUtils;
import dev.bublik.core.model.PseudoTable;
import dev.bublik.core.model.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

import static dev.bublik.cli.TestUtils.getJdbcProperties;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UpperCaseTest {
    private static int rows = 50_000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new MSSQLServerContainer("mcr.microsoft.com/mssql/server")
            .acceptLicense()
//            .withDatabaseName("test");
            .withInitScript("mssql/postgresql/sql/upperCase.sql");
    private static JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./mssql/postgresql/sql/PGupperCase.sql");

    @BeforeAll
    static void setUp() throws SQLException {
        MountableFile mf = MountableFile.forClasspathResource("./images/bublik.png");
        source.addFileSystemBind(mf.getResolvedPath(), "/bublik.png", BindMode.READ_ONLY);
        source.setPortBindings(java.util.Collections.singletonList("1433:1433"));
        source.start();
        target.setPortBindings(java.util.Collections.singletonList("5432:5432"));
        target.start();
        while (!source.isRunning() && !target.isRunning()) {
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
        target.stop();
        while (source.isRunning() || target.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void upperCase() throws Exception {
        Properties targetProp = getJdbcProperties(target);
        Table chunkTable = new PseudoTable("test", "chunk");
        Table outboxTable = new PseudoTable("public", "outbox");
        TestResult result = TestUtils.getResultCount(
                "./mssql/postgresql/yaml/mssql2pg.yaml",
                "./mssql/postgresql/json/upperCase.json",
                rows,
                sync,
                getMSSQLJdbcProperties(source),
                targetProp,
                chunkTable,
                outboxTable);
//        Thread.sleep(100_000);
        assertEquals(result.sourceCount(), result.targetCount());

        try (Connection connection = DriverManager.getConnection(targetProp.getProperty("url"), targetProp)) {
            Statement statement2 = connection.createStatement();
            ResultSet rs2 = statement2.executeQuery("select * from \"EmailAddress\"");
            assertTrue(rs2.next());
            assertEquals(1, rs2.getInt("BusinessEntityID"));
            assertEquals(101, rs2.getInt("EmailAddressID"));
            assertEquals("example@domain.com", rs2.getString("EmailAddress"));
            rs2.close();
            statement2.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Properties getMSSQLJdbcProperties(JdbcDatabaseContainer<?> db) {
        Properties properties = new Properties();
        properties.setProperty("url", db.getJdbcUrl());
        properties.setProperty("databaseName", "test");
        properties.setProperty("user", db.getUsername());
        properties.setProperty("password", db.getPassword());
        properties.setProperty("encrypt", "true");
        properties.setProperty("trustServerCertificate", "true");
        return properties;
    }
}
