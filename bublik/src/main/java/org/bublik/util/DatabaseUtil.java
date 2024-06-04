package org.bublik.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bublik.constants.SourceContext;
import org.bublik.constants.SourceContextHolder;
import org.bublik.model.ConnectionProperty;
import org.postgresql.PGConnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseUtil {

    private static DataSource fromDataSource;
    private static DataSource toDataSource;

    public static Connection getPoolConnectionDbFrom() throws SQLException {
        return getConnection(fromDataSource);
    }

    public static Connection getPoolConnectionDbTo() throws SQLException {
        return getConnection(toDataSource);
    }

    public static void closeConnection(Connection connection) throws SQLException {
        connection.close();
    }

    public static void initializeConnectionPools(ConnectionProperty property) {
        int maxPoolSize = property.getThreadCount() + 1;
        HikariConfig fromConfiguration = buildConfiguration(
                property.getFromProperty(),
                maxPoolSize,
                "from-db-hikari-pool"
        );
        fromDataSource = new HikariDataSource(fromConfiguration);

        HikariConfig toConfiguration = buildConfiguration(
                property.getToProperty(),
                maxPoolSize,
                "to-db-hikari-pool"
        );
        toDataSource = new HikariDataSource(toConfiguration);
    }

    public static void stopConnectionPools() throws SQLException {
        fromDataSource.unwrap(HikariDataSource.class).close();
        toDataSource.unwrap(HikariDataSource.class).close();
    }
    private static Connection getConnection(DataSource dataSource) throws SQLException {
        return dataSource.getConnection();
    }

    private static HikariConfig buildConfiguration(Properties property, int maxPoolSize, String poolName) {
        var hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(property.getProperty("url"));
        hikariConfig.setUsername(property.getProperty("user"));
        hikariConfig.setPassword(property.getProperty("password"));
        hikariConfig.setMaximumPoolSize(maxPoolSize);
        hikariConfig.setPoolName(poolName);
        hikariConfig.setConnectionTimeout(3000);
        return hikariConfig;
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
