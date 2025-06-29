package org.bublik.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import org.bublik.constants.ChunkStatus;
import org.bublik.model.Chunk;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.model.LogMessage;
import org.bublik.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.bublik.exception.Utils.getStackTrace;

public abstract class JDBCStorage extends Storage {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCStorage.class);
    private final DataSource dataSource;
    protected final int threadCount;

    protected JDBCStorage(StorageClass storageClass,
                          ConnectionProperty connectionProperty,
                          Boolean isSource) throws SQLException {
        super(storageClass, connectionProperty, isSource);
        this.dataSource = new HikariDataSource(
                buildConfiguration(getStorageClass().getProperties(), connectionProperty)
        );
        this.threadCount = connectionProperty.getThreadCount();
    }

/*
    public DataSource getSource() {
        return dataSource;
    }
*/

    @Override
    public Connection getConnection() throws SQLException {
        try {
            return dataSource.getConnection();
        } catch (SQLTransientConnectionException e) {
//            LOGGER.error("{}", getStackTrace(e));
            throw e;
        }
    }

    private HikariConfig buildConfiguration(Properties property, ConnectionProperty connectionProperty) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(property.getProperty("url"));
        hikariConfig.setUsername(property.getProperty("user"));
        hikariConfig.setPassword(property.getProperty("password"));
        hikariConfig.setMaximumPoolSize(connectionProperty.getThreadCount() + 1);
        hikariConfig.setConnectionTimeout(10000);
//        hikariConfig.setKeepaliveTime(30000);
//        hikariConfig.setValidationTimeout(250);
//        hikariConfig.setLeakDetectionThreshold(2000);
        hikariConfig.setAutoCommit(false);
        hikariConfig.setPoolName(getIsSource() ? "HikariPool-Source" : "HikariPool-Target");
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
        Map<Integer, Chunk<?>> chunkMap = getChunkMap(configs);
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        Properties properties = getConnectionProperty().getToProperty();
        List<Chunk<?>> chunkList = new ArrayList<>(chunkMap.values());
        chunkList.forEach(chunk -> service
                .submit(() -> {
                    Storage targetStorage = StorageService.getStorage(properties, getConnectionProperty(), false);
                    chunk.setTargetStorage(targetStorage);
                    try {
                        Chunk<?> c = chunk
                                .assignSourceConnection()
                                .setChunkStatus(ChunkStatus.ASSIGNED, null, null)
                                .assignSourceResultSet()
                                .assignResultLogMessage()
                                .setChunkStatus(ChunkStatus.PROCESSED, null, null)
                                .closeChunkSourceConnection();
                        LogMessage logMessage = c.getLogMessage();
                        logMessage.loggerChunkInfo();
                        if (chunk.getSourceConnection().isValid(0)) {
                            chunk.getSourceConnection().close();
                        }
                        assert targetStorage != null;
                        return c;
                    } catch (Exception e) {
                        LOGGER.error("ChunkId = {} {}.{} {}", chunk.getId(), chunk.getSourceTable().getSchemaName(), chunk.getSourceTable().getTableName(), getStackTrace(e));
                        try {
                            if (chunk.getSourceConnection().isValid(0)) {
                                chunk.setChunkStatus(ChunkStatus.PROCESSED_WITH_ERROR, null, getStackTrace(e));
                                chunk.getSourceConnection().close();
                            }
                        } catch (SQLException exception) {
                            LOGGER.error("{}", getStackTrace(exception));
                        }
                        assert targetStorage != null;
                        targetStorage.closeStorage();
                        throw e;
                    }
                }));
        service.shutdown();
        service.close();
    }

    @Override
    public void closeStorage(){

    }
}
