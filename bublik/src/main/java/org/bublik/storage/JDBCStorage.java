package org.bublik.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bublik.model.Chunk;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.model.LogMessage;
import org.bublik.service.ChunkService;
import org.bublik.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public abstract class JDBCStorage extends Storage implements StorageService {
    private final DataSource dataSource;
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCStorage.class);
    protected final Connection connection;
    protected final int threadCount;

    public JDBCStorage(StorageClass storageClass, ConnectionProperty connectionProperty) throws SQLException {
        super(storageClass, connectionProperty);
        this.dataSource = new HikariDataSource(
                buildConfiguration(
                        getStorageClass().getProperties(),
                        connectionProperty.getThreadCount() + 1
                )
        );
        connection = this.getConnection();
        this.threadCount = connectionProperty.getThreadCount();
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

    @Override
    public LogMessage callWorker(Chunk<?> chunk) {
        ChunkService.set(chunk);
        LogMessage logMessage;
        try (Connection chunkConnection = getConnection();
             ResultSet resultSet = chunk.getData(chunkConnection, chunk.buildFetchStatement())) {
            logMessage = chunk.getTargetStorage().transferToTarget(resultSet);
            chunk.markChunkAsProceed(chunkConnection);
        } catch (SQLException | RuntimeException e) {
            LOGGER.error("{}", e.getMessage());
            for (Throwable t : e.getSuppressed()) {
                LOGGER.error("{}", t.getMessage());
            }
            return new LogMessage (
                    0,
                    0,
                    0,
                    " UNREACHABLE TASK ",
                    chunk);
        }
        return logMessage;
    }

    @Override
    public void closeStorage(){

    }
}
