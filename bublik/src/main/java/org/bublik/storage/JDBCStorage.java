package org.bublik.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bublik.constants.ChunkStatus;
import org.bublik.model.Chunk;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.model.Table;
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
        List<Chunk<?>> chunks = new ArrayList<>(chunkMap.values());
        Storage sourceStorage = this;
        Storage targetStorage = StorageService.getStorage(properties, getConnectionProperty(), false);
        assert targetStorage != null;
        Map<Table, Table> sourceTables = configsToTables(configs);
        sourceStorage.setTables(sourceTables);
        sourceStorage.enrichSourceTables();
        targetStorage.enrichTargetTables(sourceTables);
        targetStorage.setTables(sourceTables);
        targetStorage.createTables();
        if (!sync) {
            startNOSync(chunks, targetStorage);
        } else {
            startSync(chunks, targetStorage);
        }
        targetStorage.closeStorage();
        this.closeStorage();
    }

    private void startSync(List<Chunk<?>> chunks, Storage targetStorage) throws SQLException {
        Connection sourceConnection = this.getConnection();
        sourceConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        chunks.forEach(chunk -> {
            chunk.setTargetStorage(targetStorage);
            try {
                chunk.copyChunkSync(sourceConnection, true);
            } catch (Exception e) {
                log.error("ChunkId = {} {}.{} {}", chunk.getId(), chunk.getSourceTable().getSchemaName(), chunk.getSourceTable().getTableName(), getStackTrace(e));
                try {
                    if (chunk.getSourceConnection().isValid(0)) {
                        chunk.saveChunkStatus(ChunkStatus.PROCESSED_WITH_ERROR, true, null, getStackTrace(e));
                        chunk.getSourceConnection().close();
                    }
                } catch (SQLException exception) {
                    log.error("{}", getStackTrace(exception));
                }
            }
        });
        sourceConnection.commit();
        Connection targetConnection = targetStorage.getConnection();
        targetStorage.setTables(getTables());
        targetStorage.createPrimaryKeys();
        targetStorage.createUniqueConstraints();
        targetStorage.createIndexes();
        targetStorage.createForeignKeys();
        sourceConnection.close();
        targetConnection.close();
    }

    private void startNOSync(List<Chunk<?>> chunks, Storage targetStorage) {
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        chunks.forEach(chunk -> service
                .submit(() -> {
                    chunk.setTargetStorage(targetStorage);
                    try {
                        return chunk.copyChunk(false);
                    } catch (Exception e) {
                        log.error("ChunkId = {} {}.{} {}", chunk.getId(), chunk.getSourceTable().getSchemaName(), chunk.getSourceTable().getTableName(), getStackTrace(e));
                        try {
                            if (chunk.getSourceConnection().isValid(0)) {
                                chunk.saveChunkStatus(ChunkStatus.PROCESSED_WITH_ERROR, false, null, getStackTrace(e));
                                chunk.getSourceConnection().close();
                            }
                        } catch (SQLException exception) {
                            log.error("{}", getStackTrace(exception));
                        }
                        throw e;
                    }
                })
        );
        service.shutdown();
        service.close();
    }

    @Override
    public void closeStorage() {
        if (dataSource instanceof HikariDataSource hikariDataSource) {
            hikariDataSource.close();
            log.info("HikariDataSource closed successfully.");
        } else {
            log.warn("DataSource is not an instance of HikariDataSource, cannot close.");
        }
    }

    @Override
    public void enrichTargetTables(Map<Table, Table> tables) {
        for (Map.Entry<Table, Table> entry : tables.entrySet()) {
            Table sourceTable = entry.getKey();
            Table targetTable = entry.getValue();

            targetTable.setColumns(sourceTable.getColumns());
            targetTable.setPkColumns(sourceTable.getPkColumns());
            targetTable.setIndexes(sourceTable.getIndexes());
            targetTable.setOptions(sourceTable.getOptions());
            targetTable.setUniqueConstraints(sourceTable.getUniqueConstraints());
            targetTable.setForeignKeys(sourceTable.getForeignKeys());
        }
    }

    @Override
    public boolean tableInSourceList(Table table) {
        return inList(getTables().keySet().stream().toList() , table);
    }

    @Override
    public boolean tableInTargetList(Table table) {
        return inList(getTables().values().stream().toList() , table);
    }

    private boolean inList(List<Table> tables, Table table) {
        return tables.contains(table);
    }

    @Override
    public Table getTagetTableBySourceTable(Table sourceTable) {
        for (Map.Entry<Table, Table> entry : getTables().entrySet()) {
            if (entry.getKey().equals(sourceTable)) {
                return entry.getValue();
            }
        }
        return null;
    }
}
