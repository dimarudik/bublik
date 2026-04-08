package dev.bublik.cli;

import lombok.extern.slf4j.Slf4j;
import dev.bublik.cli.addons.Utils;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Properties;

import static dev.bublik.cli.App.getConfigs;

@Slf4j
public class TestUtils {
    public static String getFilePath(String resourceFileName){
        java.net.URL cfg = TestUtils.class.getClassLoader().getResource(resourceFileName);
        if(cfg == null){
            throw new RuntimeException("file not found:"+resourceFileName);
        }
        return cfg.getFile();
    }

    public static TestResult getResult(String connectionPropertyFile,
                                       String mappingFile,
                                       int rows,
                                       boolean sync,
                                       Properties sourceProperties,
                                       Properties targetProperties) throws IOException {
        return getResult(connectionPropertyFile, mappingFile, rows, sync, sourceProperties, targetProperties, null);
    }

    public static TestResult getResult(String connectionPropertyFile,
                                       String mappingFile,
                                       int rows,
                                       boolean sync,
                                       Properties sourceProperties,
                                       Properties targetProperties,
                                       String chunkTableName) throws IOException {
        ConnectionProperty cp = Utils.connectionProperty(TestUtils.getFilePath(connectionPropertyFile));
        List<Config> configs = getConfigs(TestUtils.getFilePath(mappingFile));

        App.runProcess(cp, configs, rows, sync, chunkTableName);

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

    public static Long countRows(Properties p, String query) {
/*
        try (Connection connection =
                     DriverManager.getConnection(p.getProperty("url"), p.getProperty("user"), p.getProperty("password"))) {
*/
        try (Connection connection =
                     DriverManager.getConnection(p.getProperty("url"), p)) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            resultSet.next();
            return resultSet.getLong(1);
        }
        catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    private static String getQuery(String tableName, String whereClause) {
        return "SELECT count(1) from " + tableName + " where " + whereClause;
    }

    public static Properties getJdbcProperties(JdbcDatabaseContainer<?> db) {
        Properties properties = new Properties();
        properties.setProperty("url", db.getJdbcUrl());
        properties.setProperty("user", db.getUsername());
        properties.setProperty("password", db.getPassword());
        return properties;
    }
}
