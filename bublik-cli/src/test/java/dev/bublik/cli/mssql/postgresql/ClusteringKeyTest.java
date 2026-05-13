package dev.bublik.cli.mssql.postgresql;

import dev.bublik.cli.TestResult;
import dev.bublik.cli.TestUtils;
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

public class ClusteringKeyTest {
    private static int rows = 50_000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new MSSQLServerContainer("mcr.microsoft.com/mssql/server")
            .acceptLicense()
//            .withDatabaseName("test");
            .withInitScript("mssql/postgresql/sql/mssql-ClusteringKey.sql");
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

    // Реализовать проверку контрольной сумма кластерного ключа:
    // SELECT COUNT(1), SUM(id1), SUM(id2) FROM t4;
    @Test
    void clusteringKey() throws InterruptedException, IOException {
        TestResult result = TestUtils.getResultCount(
                "./mssql/postgresql/yaml/mssql2pg.yaml",
                "./mssql/postgresql/json/clusteringKey.json",
                rows,
                sync,
                getMSSQLJdbcProperties(source),
                getJdbcProperties(target));
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
