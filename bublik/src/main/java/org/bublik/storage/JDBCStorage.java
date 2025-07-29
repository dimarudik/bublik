package org.bublik.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bublik.constants.ChunkStatus;
import org.bublik.model.Chunk;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
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
    private static final Logger log = LoggerFactory.getLogger(JDBCStorage.class);
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

    @Override
    public Connection getConnection() throws SQLException {
        try {
            return dataSource.getConnection();
        } catch (SQLTransientConnectionException e) {
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
    public void start(List<Config> configs, boolean sync) throws SQLException {
        Map<Integer, Chunk<?>> chunkMap = getChunkMap(configs);
        Properties properties = getConnectionProperty().getToProperty();
        List<Chunk<?>> chunkList = new ArrayList<>(chunkMap.values());
        if (!sync) {
            ExecutorService service = Executors.newFixedThreadPool(threadCount);
            chunkList.forEach(chunk -> service
                    .submit(() -> {
                        Storage targetStorage = StorageService.getStorage(properties, getConnectionProperty(), false);
                        chunk.setTargetStorage(targetStorage);
                        try {
                            return chunk.copyChunk(sync);
                        } catch (Exception e) {
                            log.error("ChunkId = {} {}.{} {}", chunk.getId(), chunk.getSourceTable().getSchemaName(), chunk.getSourceTable().getTableName(), getStackTrace(e));
                            try {
                                if (chunk.getSourceConnection().isValid(0)) {
                                    chunk.saveChunkStatus(ChunkStatus.PROCESSED_WITH_ERROR, sync, null, getStackTrace(e));
                                    chunk.getSourceConnection().close();
                                }
                            } catch (SQLException exception) {
                                log.error("{}", getStackTrace(exception));
                            }
                            assert targetStorage != null;
                            targetStorage.closeStorage();
                            throw e;
                        }
                    })
            );
            service.shutdown();
            service.close();
        } else {
            Storage targetStorage = StorageService.getStorage(properties, getConnectionProperty(), false);
            Connection sourceConnection = this.getConnection();
            sourceConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            chunkList.forEach(chunk -> {
                chunk.setTargetStorage(targetStorage);
                try {
                    chunk.copyChunkInSync(sourceConnection, sync);
                } catch (Exception e) {
                    log.error("ChunkId = {} {}.{} {}", chunk.getId(), chunk.getSourceTable().getSchemaName(), chunk.getSourceTable().getTableName(), getStackTrace(e));
                    try {
                        if (chunk.getSourceConnection().isValid(0)) {
                            chunk.saveChunkStatus(ChunkStatus.PROCESSED_WITH_ERROR, sync, null, getStackTrace(e));
                            chunk.getSourceConnection().close();
                        }
                    } catch (SQLException exception) {
                        log.error("{}", getStackTrace(exception));
                    }
                }
            });
            sourceConnection.commit();
            sourceConnection.close();
            assert targetStorage != null;
            targetStorage.closeStorage();
        }
    }

    @Override
    public void closeStorage(){

    }
}
