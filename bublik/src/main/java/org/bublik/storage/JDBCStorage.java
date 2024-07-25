package org.bublik.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bublik.constants.ChunkStatus;
import org.bublik.model.*;
import org.bublik.service.ChunkService;
import org.bublik.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class JDBCStorage extends Storage implements StorageService {
    private final DataSource dataSource;
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCStorage.class);
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
    public void start(List<Config> configs) throws SQLException {
        if (hook(configs)) {
            Map<Integer, Chunk<?>> chunkMap = getChunkMap(configs);
            initialConnection.close();
            ExecutorService service = Executors.newFixedThreadPool(threadCount);
/*
            chunkMap.forEach((key, chunk) ->
                    CompletableFuture
                        .supplyAsync(() -> callWorker(chunk), service)
                        .thenAccept(LogMessage::loggerChunkInfo));
*/
            chunkMap.forEach((key, chunk) ->
                    CompletableFuture
                            .supplyAsync(() -> getSourceResultSet(chunk), service)
                            .thenApply(this::getLogMessage)
                            .thenAccept(LogMessage::loggerChunkInfo));
            service.shutdown();
            service.close();
        }
    }

    @Override
    public LogMessage callWorker(Chunk<?> chunk) {
        ChunkService.set(chunk);
        LogMessage logMessage;
        try (Connection sourceConnection = getConnection()) {
            chunk.setChunkStatus(sourceConnection, ChunkStatus.ASSIGNED);
            ResultSet resultSet = chunk.getData(sourceConnection, chunk.buildFetchStatement());
            logMessage = chunk.getTargetStorage().transferToTarget(resultSet);
            resultSet.close();
            chunk.setChunkStatus(sourceConnection, ChunkStatus.PROCESSED);
        } catch (SQLException | RuntimeException e) {
            LOGGER.error("{}", e.getMessage());
            for (Throwable t : e.getSuppressed()) {
                LOGGER.error("{}", t.getMessage());
            }
            return new LogMessage (0, 0, 0, " UNREACHABLE TASK ", chunk);
        }
        return logMessage;
    }

    public ResultSet getSourceResultSet(Chunk<?> chunk) {
        ChunkService.set(chunk);
        try {
            Connection chunkConnection = getConnection();
            chunk.setSourceConnection(chunkConnection);
            chunk.setChunkStatus(chunkConnection, ChunkStatus.ASSIGNED);
            return chunk.getData(chunkConnection, chunk.buildFetchStatement());
        } catch (SQLException | RuntimeException e) {
            LOGGER.error("{}", e.getMessage());
            for (Throwable t : e.getSuppressed()) {
                LOGGER.error("{}", t.getMessage());
            }
            throw new RuntimeException();
        }
    }

    public LogMessage getLogMessage(ResultSet resultSet) {
        Chunk<?> chunk = ChunkService.get();
        try {
            LogMessage logMessage = chunk.getTargetStorage().transferToTarget(resultSet);
            resultSet.close();
            Connection sourceConnection = chunk.getSourceConnection();
            chunk.setChunkStatus(sourceConnection, ChunkStatus.PROCESSED);
            sourceConnection.close();
            return logMessage;
        } catch (SQLException | RuntimeException e) {
            LOGGER.error("{}", e.getMessage());
            for (Throwable t : e.getSuppressed()) {
                LOGGER.error("{}", t.getMessage());
            }
            return new LogMessage (0, 0, 0, " UNREACHABLE TASK ", chunk);
        }
    }

    @Override
    public void closeStorage(){

    }
}
