package dev.bublik.cli.mssql.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.cli.TestUtils;
import dev.bublik.core.model.PseudoTable;
import dev.bublik.core.model.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import static dev.bublik.cli.TestUtils.getJdbcProperties;
import static dev.bublik.cli.TestUtils.getResultCount;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AllTypesTest {
    private static int rows = 50_000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new MSSQLServerContainer("mcr.microsoft.com/mssql/server")
            .acceptLicense()
            .withInitScript("mssql/postgresql/sql/mssql-allTypes.sql");
    private static JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./mssql/postgresql/sql/pg-init.sql");

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
    void allTypes() throws Exception {
        Table chunkTable = new PseudoTable("test", "chunk");
        Table outboxTable = new PseudoTable("public", "outbox");
        TestResult result = TestUtils.getResultCount(
                "./mssql/postgresql/yaml/mssql2pg.yaml",
                "./mssql/postgresql/json/allTypes.json",
                rows,
                sync,
                getMSSQLJdbcProperties(source),
                getJdbcProperties(target),
                chunkTable,
                outboxTable);
//        Thread.sleep(1_000_000);
        assertEquals(result.sourceCount(), result.targetCount());
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
