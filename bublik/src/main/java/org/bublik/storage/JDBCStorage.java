package org.bublik.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bublik.constants.ChunkStatus;
import org.bublik.model.Chunk;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.model.LogMessage;
import org.bublik.service.StorageService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class JDBCStorage extends Storage {
    private final DataSource dataSource;
    protected final Connection initialConnection;
    protected final int threadCount;

    public JDBCStorage(StorageClass storageClass, ConnectionProperty connectionProperty) throws SQLException {
        super(storageClass, connectionProperty);
        this.dataSource = new HikariDataSource(
                buildConfiguration(
                        getStorageClass().getProperties(),
                        connectionProperty.getThreadCount() + 1
                )
        );
        initialConnection = this.getConnection();
        this.threadCount = connectionProperty.getThreadCount();
    }

    public DataSource getSource() {
        return dataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return getSource().getConnection();
    }

    private HikariConfig buildConfiguration(Properties property, int maxPoolSize) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(property.getProperty("url"));
        hikariConfig.setUsername(property.getProperty("user"));
        hikariConfig.setPassword(property.getProperty("password"));
        hikariConfig.setMaximumPoolSize(maxPoolSize);
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
    public void start(List<Config> configs) throws SQLException {
//        if (hook(configs)) {
            Map<Integer, Chunk<?>> chunkMap = getChunkMap(configs);
            initialConnection.close();
            ExecutorService service = Executors.newFixedThreadPool(threadCount);
            List<Chunk<?>> chunkList = new ArrayList<>(chunkMap.values());
            chunkList.forEach(chunk -> service
                    .submit(() -> {
                        Storage targetStorage = StorageService.getStorage(getConnectionProperty().getToProperty(), getConnectionProperty());
                        chunk.setTargetStorage(targetStorage);
                        Chunk<?> c = chunk
                                .assignSourceConnection()
                                .setChunkStatus(ChunkStatus.ASSIGNED)
                                .assignSourceResultSet()
                                .assignResultLogMessage()
                                .setChunkStatus(ChunkStatus.PROCESSED)
                                .closeChunkSourceConnection();
                        LogMessage logMessage = c.getLogMessage();
                        logMessage.loggerChunkInfo();
                        assert targetStorage != null;
                        targetStorage.closeStorage();
                        return c;
                    }));
            service.shutdown();
            service.close();
//        }
    }

    @Override
    public void closeStorage(){

    }
}
