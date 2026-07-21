package dev.bublik.mssql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.PseudoTable;
import dev.bublik.core.model.Table;
import dev.bublik.core.storage.Storage;
import dev.bublik.mssql.storage.MSSQLStorage;
import dev.bublik.postgres.storage.PostgresStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PostgresMigrationTest {
    static final JdbcDatabaseContainer<?> mssql = new MSSQLServerContainer(
            DockerImageName.parse("mcr.microsoft.com/mssql/server"))
            .acceptLicense();

    static final JdbcDatabaseContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres"));

    static HikariDataSource sourceDataSource;
    static HikariDataSource targetDataSource;

    @BeforeAll
    static void beforeAll() throws Exception {
        mssql.start();
        postgres.start();

        HikariConfig sourceConfig = new HikariConfig();
        sourceConfig.setJdbcUrl(mssql.getJdbcUrl());
        sourceConfig.setUsername(mssql.getUsername());
        sourceConfig.setPassword(mssql.getPassword());
        sourceConfig.setMaximumPoolSize(5);
        sourceDataSource = new HikariDataSource(sourceConfig);

        HikariConfig targetConfig = new HikariConfig();
        targetConfig.setJdbcUrl(postgres.getJdbcUrl());
        targetConfig.setUsername(postgres.getUsername());
        targetConfig.setPassword(postgres.getPassword());
        targetConfig.setMaximumPoolSize(5);
        targetDataSource = new HikariDataSource(targetConfig);

        try (Connection conn = sourceDataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE source_users (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(100))");

            stmt.execute("INSERT INTO source_users (name) VALUES ('Alice')");
            stmt.execute("INSERT INTO source_users (name) VALUES ('Bob')");
            stmt.execute("INSERT INTO source_users (name) VALUES ('Charlie')");
        }

        try (Connection conn = targetDataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE target_users (id INT PRIMARY KEY, name VARCHAR(100))");
        }
    }

    @AfterAll
    static void afterAll() {
        if (sourceDataSource != null) sourceDataSource.close();
        if (targetDataSource != null) targetDataSource.close();
        mssql.stop();
        postgres.stop();
    }

    @Test
    void testMssqlToPostgresMigration() throws Exception {
        Storage sourceStorage = new MSSQLStorage(sourceDataSource);
        Storage targetStorage = new PostgresStorage(targetDataSource);

        List<Config> configs = new ArrayList<>();
        Config config = Config.builder()
                .from("dbo", "source_users")
                .to("public", "target_users")
                .build();
        configs.add(config);

        sourceStorage.start(targetStorage, configs, 1000);

        try (Connection conn = targetDataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*), MIN(name) FROM target_users")) {

            assertTrue(rs.next());
            int count = rs.getInt(1);
            String firstUser = rs.getString(2);

            assertEquals(3, count, "Количество перенесенных строк в PostgreSQL должно быть равно 3");
            assertEquals("Alice", firstUser, "Данные внутри PostgreSQL должны совпадать");
        }

        sourceStorage.closeStorage();
        targetStorage.closeStorage();
    }
}
