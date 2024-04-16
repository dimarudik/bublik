package org.example.util;

import org.example.constants.SourceContext;
import org.example.constants.SourceContextHolder;
import org.postgresql.PGConnection;
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

    public static SourceContextHolder sourceContextHolder(Connection connection) throws SQLException {
        if (connection.isWrapperFor(oracle.jdbc.OracleConnection.class)) {
            return new SourceContextHolder(SourceContext.Oracle);
        } else if (connection.isWrapperFor(PGConnection.class)) {
            return new SourceContextHolder(SourceContext.PostgreSQL);
        }
        throw new SQLException("Unknown DataSource");
    }
}
