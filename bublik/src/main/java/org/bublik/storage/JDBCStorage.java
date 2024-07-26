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
            chunkMap.forEach((key, chunk) -> service
                    .execute(() -> {
                        Chunk<?> chunk1 = getChunkSourceConnection(chunk);
                        Chunk<?> chunk2 = setChunkStatus(chunk1, ChunkStatus.ASSIGNED);
                        Chunk<?> chunk3 = getSourceResultSet(chunk2);
                        Chunk<?> chunk4 = getChunkLogMessage(chunk3);
                        Chunk<?> chunk5 = setChunkStatus(chunk4, ChunkStatus.PROCESSED);
                        Chunk<?> chunk6 = closeChunkSourceConnection(chunk5);
                        LogMessage logMessage = chunk6.getLogMessage();
                        logMessage.loggerChunkInfo();
                    }));
/*
            chunkMap.forEach((key, chunk) ->
                    CompletableFuture
                            .supplyAsync(() -> getChunkSourceConnection(chunk), service)
                            .thenApply(ch -> setChunkStatus(ch, ChunkStatus.ASSIGNED))
                            .thenApply(this::getSourceResultSet)
                            .thenApply(this::getChunkLogMessage)
                            .thenApply(ch -> setChunkStatus(ch, ChunkStatus.PROCESSED))
                            .thenApply(this::closeChunkSourceConnection)
                            .thenApply(Chunk::getLogMessage)
                            .thenAccept(LogMessage::loggerChunkInfo));
*/
            service.shutdown();
            service.close();
        }
    }

    public Chunk<?> getChunkSourceConnection(Chunk<?> chunk) {
//        LOGGER.info(" ...");
        try {
            Connection sourceConnection = getConnection();
            chunk.setSourceConnection(sourceConnection);
            return chunk;
        } catch (SQLException | RuntimeException e) {
            LOGGER.error("{}", e.getMessage());
            for (Throwable t : e.getSuppressed()) {
                LOGGER.error("{}", t.getMessage());
            }
            throw new RuntimeException();
        }
    }

    public Chunk<?> setChunkStatus(Chunk<?> chunk, ChunkStatus status) {
        try {
            chunk.setChunkStatus(chunk.getSourceConnection(), status);
            return chunk;
        } catch (SQLException | RuntimeException e) {
            LOGGER.error("{}", e.getMessage());
            for (Throwable t : e.getSuppressed()) {
                LOGGER.error("{}", t.getMessage());
            }
            throw new RuntimeException();
        }
    }

    public Chunk<?> getSourceResultSet(Chunk<?> chunk) {
        try {
            ResultSet resultSet = chunk.getData(chunk.getSourceConnection(), chunk.buildFetchStatement());
            chunk.setResultSet(resultSet);
            return chunk;
        } catch (SQLException | RuntimeException e) {
            LOGGER.error("{}", e.getMessage());
            for (Throwable t : e.getSuppressed()) {
                LOGGER.error("{}", t.getMessage());
            }
            throw new RuntimeException();
        }
    }

    public Chunk<?> getChunkLogMessage(Chunk<?> chunk) {
        try {
            LogMessage logMessage = chunk.getTargetStorage().transferToTarget(chunk);
            chunk.setLogMessage(logMessage);
            ResultSet resultSet = chunk.getResultSet();
            resultSet.close();
            return chunk;
        } catch (SQLException | RuntimeException e) {
            LOGGER.error("{}", e.getMessage());
            for (Throwable t : e.getSuppressed()) {
                LOGGER.error("{}", t.getMessage());
            }
            chunk.setLogMessage(new LogMessage (0, 0, 0, " UNREACHABLE TASK ", chunk));
            return chunk;
        }
    }

    public Chunk<?> closeChunkSourceConnection(Chunk<?> chunk) {
        try {
            Connection connection = chunk.getSourceConnection();
            connection.close();
            return chunk;
        } catch (SQLException | RuntimeException e) {
            LOGGER.error("{}", e.getMessage());
            for (Throwable t : e.getSuppressed()) {
                LOGGER.error("{}", t.getMessage());
            }
            throw new RuntimeException();
        }
    }

    @Override
    public void closeStorage(){

    }
}
