package org.bublik.core.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.model.Chunk;
import org.bublik.core.model.Config;
import org.bublik.core.model.ConnectionProperty;
import org.bublik.core.model.Table;
import org.bublik.core.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.bublik.core.util.Utils.getStackTrace;

public abstract class JDBCStorage extends Storage {
    private static final Logger log = LoggerFactory.getLogger(JDBCStorage.class);
    private final DataSource dataSource;
    protected final int threadCount;

    protected JDBCStorage(StorageClass storageClass, ConnectionProperty connectionProperty) throws SQLException {
        super(storageClass, connectionProperty);
        HikariConfig hikariConfig = buildConfiguration(getStorageClass().getProperties(), connectionProperty);
        this.dataSource = new HikariDataSource(hikariConfig);
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

    private HikariConfig buildConfiguration(Properties property, ConnectionProperty connectionProperty) throws SQLException {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(property.getProperty("url"));
        hikariConfig.setUsername(property.getProperty("user"));
        hikariConfig.setPassword(property.getProperty("password"));
        hikariConfig.setMaximumPoolSize(connectionProperty.getThreadCount() + 1);
        hikariConfig.setConnectionTimeout(10000);
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
    public void start(List<Config> cfgs, boolean sync, int rows) throws SQLException {
        List<Config> configs = new ArrayList<>();
        for (Config c : cfgs) {
            configs.add(c.copy());
        }
        Storage sourceStorage = this;
        Storage targetStorage = StorageService.getStorage(getConnectionProperty().getToProperty(), getConnectionProperty());
        createChunks(configs, sync, rows);
        assert targetStorage != null;
        targetStorage.createOutbox();
        Map<Table, Table> sourceTables = configsToTables(configs, targetStorage);
        sourceStorage.setTables(sourceTables);
        sourceStorage.enrichSourceTables();
        targetStorage.enrichTargetTables(sourceTables);
        targetStorage.setTables(sourceTables);
        if (sourceStorage.getClass().equals(targetStorage.getClass())) {
            targetStorage.createTables();
        }
        if (!sync) {
            startNOSync(targetStorage, configs);
        } else {
            try {
                startSync(targetStorage, configs, rows);
            } catch (Exception e) {
                log.info("{}", getStackTrace(e));
                targetStorage.closeStorage();
                this.closeStorage();
            }
        }
        targetStorage.closeStorage();
        this.closeStorage();
    }

    private void startNOSync(Storage targetStorage, List<Config> configs) throws SQLException {
        Map<Integer, Chunk<?>> chunkMap = getChunkMap(configs);
        List<Chunk<?>> chunks = new ArrayList<>(chunkMap.values());
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

    private void startSync(Storage targetStorage, List<Config> configs, int rows) throws SQLException {
        Connection sourceConnection = this.getConnection();
        sourceConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        Map.Entry<String,Long> lsnXid = getSystemChangeNumberWithTrxId();
        log.info("{} {}", lsnXid.getKey(), lsnXid.getValue());
        createChunks(configs, true, rows);

//        fillCtidChunksV2(configs, sourceConnection, rows, true);
        Map<Integer, Chunk<?>> chunkMap = getChunkMap(configs, sourceConnection);
        List<Chunk<?>> chunks = new ArrayList<>(chunkMap.values());
        chunks.forEach(chunk -> {
            chunk.setTargetStorage(targetStorage);
            try {
                chunk.copyChunkSync(sourceConnection, true);
            } catch (Exception e) {
                log.error("ChunkId = {} {}.{} {}", chunk.getId(), chunk.getSourceTable().getSchemaName(), chunk.getSourceTable().getTableName(), getStackTrace(e));
                throw new RuntimeException(e);
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

    @Override
    public Table getSourceTableByTargetTable(Table targetTable) {
        for (Map.Entry<Table, Table> entry : getTables().entrySet()) {
            if (entry.getValue().equals(targetTable)) {
                return entry.getKey();
            }
        }
        return null;
    }

    @Override
    public Map<Table, Table> configsToTables(List<Config> configs, Storage targetStorage) {
        Map<Table, Table> tables = new HashMap<>();
        for (Config c : configs) {
            tables.put(configToTable(c.fromSchemaName(), c.fromTableName()), targetStorage.configToTable(c.toSchemaName(), c.toTableName()));
        }
        return tables;
    }
}
