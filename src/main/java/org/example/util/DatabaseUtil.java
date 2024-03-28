package org.example.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseUtil {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseUtil.class);

    public static Connection getConnection(Properties properties) {
        Connection connection = null;
        try {
            DriverManager.setLoginTimeout(1);
            connection = DriverManager.getConnection(properties.getProperty("url"), properties);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
        return connection;
    }

    public static void closeConnection(Connection connection) throws SQLException {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
