package org.example.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.example.model.SourceTargetProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseUtil {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseUtil.class);

    private static DataSource fromDataSource;
    private static DataSource toDataSource;

    public static Connection getConnectionDbFrom() {
        return getConnection(fromDataSource);
    }

    public static Connection getConnectionDbTo() {
        return getConnection(toDataSource);
    }

    public static void closeConnection(Connection connection) throws SQLException {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void initializeConnectionPools(SourceTargetProperties properties) {
        var maxPoolSize = properties.getThreadCount();
        var fromConfiguration = buildConfiguration(
                properties.getFromProperty(),
                maxPoolSize,
                "from-db-hikari-pool"
        );
        fromDataSource = new HikariDataSource(fromConfiguration);

        var toConfiguration = buildConfiguration(
                properties.getToProperty(),
                maxPoolSize,
                "to-db-hikari-pool"
        );
        toDataSource = new HikariDataSource(toConfiguration);
    }

    private static Connection getConnection(DataSource dataSource) {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            logger.error("Failed to get connection from pool: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private static HikariConfig buildConfiguration(Properties property, int maxPoolSize, String poolName) {
        var hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(property.getProperty("url"));
        hikariConfig.setUsername(property.getProperty("user"));
        hikariConfig.setPassword(property.getProperty("password"));
        hikariConfig.setMaximumPoolSize(maxPoolSize);
        hikariConfig.setPoolName(poolName);
        return hikariConfig;
    }
}
