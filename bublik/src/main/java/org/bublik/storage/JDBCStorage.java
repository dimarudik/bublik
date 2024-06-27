package org.bublik.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bublik.model.Chunk;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.model.LogMessage;
import org.bublik.service.ChunkService;
import org.bublik.service.StorageService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class JDBCStorage extends Storage implements StorageService {
    private final DataSource dataSource;
    private Storage targetStorage;

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

    public void setTargetStorage(Storage targetStorage) {
        this.targetStorage = targetStorage;
    }

    public Storage getTargetStorage() {
        return targetStorage;
    }

    @Override
    public LogMessage callWorker(Chunk<?> chunk, Map<String, Integer> columnsFromDB) throws SQLException {
        Connection chunkConnection = getConnection();
        String query = chunk.buildFetchStatement(columnsFromDB);
        ResultSet resultSet = chunk.getData(chunkConnection, query);
        ChunkService.set(chunk);
        LogMessage logMessage;
        try {
            logMessage = chunk.getTargetStorage().transferToTarget(resultSet);
        } catch (SQLException | RuntimeException e) {
            resultSet.close();
            chunkConnection.close();
            throw e;
        }
        resultSet.close();
        chunk.markChunkAsProceed(chunkConnection);
        chunkConnection.close();
        return logMessage;
    }
}
