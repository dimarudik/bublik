package dev.bublik.cli;

import dev.bublik.core.model.PseudoTable;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageService;
import lombok.extern.slf4j.Slf4j;
import dev.bublik.cli.addons.Utils;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.List;
import java.util.Properties;

import static dev.bublik.cli.App.getConfigs;

@Slf4j
public class TestUtils {
    static public Table chunkTable = new PseudoTable("public", "_chunk");
    static public Table outboxTable = new PseudoTable("public", "_outbox");

    static public Table chunkTable2 = new PseudoTable("public", "_chunk2");
    static public Table outboxTable2 = new PseudoTable("public", "_outbox2");

    public static String getFilePath(String resourceFileName){
        java.net.URL cfg = TestUtils.class.getClassLoader().getResource(resourceFileName);
        if(cfg == null){
            throw new RuntimeException("file not found:"+resourceFileName);
        }
        return cfg.getFile();
    }

    public static TestResult getResultCount(String connectionPropertyFile,
                                            String mappingFile,
                                            int rows,
                                            boolean sync,
                                            Properties sourceProperties,
                                            Properties targetProperties) throws IOException, SQLException {
        return getResultCount(connectionPropertyFile, mappingFile, rows, sync, sourceProperties, targetProperties, chunkTable, outboxTable);
    }

    public static TestResult getResultCount(String connectionPropertyFile,
                                            String mappingFile,
                                            int rows,
                                            boolean sync,
                                            Properties sourceProperties,
                                            Properties targetProperties,
                                            Table chunkTable,
                                            Table outboxTable) throws IOException, SQLException {
        ConnectionProperty cp = Utils.connectionProperty(TestUtils.getFilePath(connectionPropertyFile));
        List<Config> configs = getConfigs(TestUtils.getFilePath(mappingFile));

//        App.runProcess(cp, configs, rows);
        StorageService.init(cp, configs, rows, chunkTable, outboxTable);

        long sourceCount = 0;
        long targetCount = 0;
        for (Config config : configs) {
            String fromQuery = getQuery(config.fromSchemaName() + "." + config.fromTableName(),
                    config.fetchWhereClause() == null ? " 1 = 1 " : config.fetchWhereClause());
            String toQuery = getQuery(
                    (config.toSchemaName() == null ? config.fromSchemaName() + "." : config.toSchemaName() + ".")
                            + (config.toTableName() == null ? config.fromTableName() : config.toTableName()),
                    " 1 = 1 ");
            System.out.println(fromQuery);
            sourceCount += TestUtils.countRows(sourceProperties, fromQuery);
            System.out.println(toQuery);
            targetCount += TestUtils.countRows(targetProperties, toQuery);
        }
        return new TestResult(sourceCount, targetCount);
    }

    private long getColumnChecksum(Connection conn, String tableName, String columnName) throws SQLException {
        String sql = String.format(
                "SELECT COALESCE(SUM(('x' || substring(md5(coalesce(%s::text, '')), 1, 16))::bit(64)::bigint), 0) FROM %s",
                columnName, tableName
        );

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }

/*
    public static Long countRows(Properties p, String query) {
        Properties cleanProps = new Properties();
        cleanProps.putAll(p);
        cleanProps.remove("url");
        try (Connection connection =
                     DriverManager.getConnection(p.getProperty("url"), cleanProps)) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            resultSet.next();
            return resultSet.getLong(1);
        }
        catch (SQLException e){
            throw new RuntimeException(e);
        }
    }
*/
    public static Long countRows(Properties p, String query) {
        String rawUrl = p.getProperty("url");

        // Если это запрос к Oracle/Postgres, оставляем стандартный JDBC-путь
        if (!rawUrl.contains("jdbc:clickhouse:http")) {
            Properties cleanProps = new Properties();
            cleanProps.putAll(p);
            cleanProps.remove("url");
            try (java.sql.Connection connection = java.sql.DriverManager.getConnection(rawUrl, cleanProps);
                 java.sql.Statement statement = connection.createStatement();
                 java.sql.ResultSet resultSet = statement.executeQuery(query)) {
                resultSet.next();
                return resultSet.getLong(1);
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
        }

        // Для ClickHouse делаем прямой, независимый HTTP-запрос
        try {
            // Извлекаем базовый хост (например, http://localhost:8123/)
            String httpUrl = rawUrl.replace("jdbc:clickhouse:", "");
            if (httpUrl.contains("?")) {
                httpUrl = httpUrl.substring(0, httpUrl.indexOf("?"));
            }

            // Кодируем SQL-запрос для передачи в URL
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
            String finalUrl = httpUrl + "?query=" + encodedQuery;

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(finalUrl))
                    .header("X-ClickHouse-User", p.getProperty("user", "default"))
                    .header("X-ClickHouse-Key", p.getProperty("password", ""))
                    // Принудительно отключаем прогресс-заголовки на уровне HTTP-протокола
                    .header("X-ClickHouse-No-Progress", "1")
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("ClickHouse HTTP query failed with status " + response.statusCode() + ": " + response.body());
            }

            // ClickHouse возвращает число со знаком переноса строки (например, "1\n"), очищаем его
            return Long.parseLong(response.body().trim());

        } catch (Exception e) {
            throw new RuntimeException("Failed to count rows via ClickHouse HTTP API", e);
        }
    }

    public static String getQuery(String tableName, String whereClause) {
        return "SELECT count(1) from " + tableName + " where " + whereClause;
    }

    public static Properties getJdbcProperties(JdbcDatabaseContainer<?> db) {
        Properties properties = new Properties();
        properties.setProperty("url", db.getJdbcUrl());
//        properties.setProperty("database", db.getDatabaseName());
        properties.setProperty("user", db.getUsername());
        properties.setProperty("password", db.getPassword());
        return properties;
    }

    public static Properties getFullJdbcProperties(JdbcDatabaseContainer<?> target) {
        String url = target.getJdbcUrl();
//        System.out.println(url);
        Properties properties = new Properties();
        properties.setProperty("url", url);
        properties.setProperty("database", target.getDatabaseName());
        properties.setProperty("user", target.getUsername());
        properties.setProperty("password", target.getPassword());
        return properties;
    }
}
