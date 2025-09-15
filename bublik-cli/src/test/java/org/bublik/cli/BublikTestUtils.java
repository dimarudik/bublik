package org.bublik.cli;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.bublik.core.model.ConnectionProperty;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class BublikTestUtils {
    public static String getFilePath(String resourceFileName){
        java.net.URL cfg = PostgresToPostgresTest.class.getClassLoader().getResource(resourceFileName);
        if(cfg == null){
            throw new RuntimeException("file not found:"+resourceFileName);
        }
        return cfg.getFile();
    }

    public static ConnectionProperty buildConnectionProperty(PostgreSQLContainer from, PostgreSQLContainer to){
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

    public static Long countRows(JdbcDatabaseContainer db, String tableName){
        String jdbcUrl = db.getJdbcUrl();
        String username = db.getUsername();
        String password = db.getPassword();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT count(*) from "+tableName);
            resultSet.next();
            return resultSet.getLong(1);
        }
        catch (SQLException e){
            throw new RuntimeException(e);
        }
    }
}
