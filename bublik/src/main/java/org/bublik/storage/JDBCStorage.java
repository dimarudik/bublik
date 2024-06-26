package org.bublik.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.service.StorageService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public abstract class JDBCStorage extends Storage implements StorageService {
    private final DataSource dataSource;

    public JDBCStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        this.dataSource = new HikariDataSource(
                buildConfiguration(
                        getStorageClass().getProperties(),
                        connectionProperty.getThreadCount() + 1
                )
        );
    }

    public DataSource getSource() {
        return dataSource;
    }

    public Connection getConnection() throws SQLException {
        return getSource().getConnection();
    }

    private HikariConfig buildConfiguration(Properties property, int maxPoolSize) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(property.getProperty("url"));
        hikariConfig.setUsername(property.getProperty("user"));
        hikariConfig.setPassword(property.getProperty("password"));
        hikariConfig.setMaximumPoolSize(maxPoolSize);
//        hikariConfig.setPoolName(poolName);
        hikariConfig.setConnectionTimeout(3000);
        hikariConfig.setAutoCommit(false);
        return hikariConfig;
    }

    public Config findByTaskName(List<Config> configs, String taskName) {
        for (Config config : configs) {
            if (config.fromTaskName().equals(taskName)) {
                return config;
            }
        }
        return null;
    }
}
