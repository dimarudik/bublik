package org.bublik.cli;

import lombok.extern.slf4j.Slf4j;
import org.bublik.cli.addons.Utils;
import org.bublik.core.model.Config;
import org.bublik.core.model.ConnectionProperty;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.io.IOException;
import java.sql.*;
import java.util.List;

import static org.bublik.cli.App.getConfigs;

@Slf4j
public class TestUtils {
    public static String getFilePath(String resourceFileName){
        java.net.URL cfg = PostgresToPostgresTest.class.getClassLoader().getResource(resourceFileName);
        if(cfg == null){
            throw new RuntimeException("file not found:"+resourceFileName);
        }
        return cfg.getFile();
    }

/*
    public static ConnectionProperty buildConnectionProperty(JdbcDatabaseContainer from, JdbcDatabaseContainer to){
        ConnectionProperty connectionProperty = new ConnectionProperty();
        connectionProperty.setThreadCount(10);
        connectionProperty.setToProperties(buildConnectionMap(to));
        connectionProperty.setFromProperties(buildConnectionMap(from));
        return connectionProperty;
    }

    public static Map<String, String> buildConnectionMap(JdbcDatabaseContainer from){
        Map<String, String> fromUrlMap = new HashMap<>();
        fromUrlMap.put("url", from.getJdbcUrl());
        fromUrlMap.put("user", from.getUsername());
        fromUrlMap.put("password",from.getPassword());
        return fromUrlMap;
    }
*/

    protected static TestResult getResult(String connectionPropertyFile,
                                String mappingFile,
                                int rows,
                                boolean sync,
                                JdbcDatabaseContainer<?> source,
                                JdbcDatabaseContainer<?> target) throws IOException {
        ConnectionProperty cp = Utils.connectionProperty(TestUtils.getFilePath(connectionPropertyFile));
        List<Config> configs = getConfigs(TestUtils.getFilePath(mappingFile));
        Config c = configs.getFirst();

        App.runProcess(cp, configs, rows, sync);

        Long sourceCount = TestUtils.countRows(source, c.fromSchemaName() + "." + c.fromTableName());
        Long targetCount = TestUtils.countRows(target,
                (c.toSchemaName() == null ? c.fromSchemaName() : c.toSchemaName())
                        + "."
                        + (c.toTableName() == null ? c.fromTableName() : c.toTableName()) );
        return new TestResult(sourceCount, targetCount);
    }

    public static Long countRows(JdbcDatabaseContainer db, String tableName){
        String jdbcUrl = db.getJdbcUrl();
        String username = db.getUsername();
        String password = db.getPassword();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            Statement statement = connection.createStatement();
            String query = "SELECT count(1) from " + tableName;
//            log.info("Executing query: {}", query);
            ResultSet resultSet = statement.executeQuery(query);
            resultSet.next();
            return resultSet.getLong(1);
        }
        catch (SQLException e){
            throw new RuntimeException(e);
        }
    }
}
