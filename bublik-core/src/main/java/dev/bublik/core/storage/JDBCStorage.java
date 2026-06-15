package dev.bublik.core.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.model.*;
import dev.bublik.core.service.JDBCStorageService;
import dev.bublik.core.service.Source;
import dev.bublik.core.service.Target;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static dev.bublik.core.util.Utils.getStackTrace;

public abstract class JDBCStorage<K, T, S extends Connection, R> extends Storage<K, T, S, R>
        implements JDBCStorageService<K, T, S, R>, Source, Target {
    private static final Logger log = LoggerFactory.getLogger(JDBCStorage.class);
    private final DataSource dataSource;
    protected final int threadCount;
    private Connection connection;

    public JDBCStorage(ConnectionProperty connectionProperty) throws SQLException{
        super(connectionProperty);
        HikariConfig hikariConfig = buildConfiguration(getStorageClass().getProperties(), connectionProperty);
        this.dataSource = new HikariDataSource(hikariConfig);
        this.threadCount = connectionProperty.getThreadCount();
    }

    protected JDBCStorage(StorageClass storageClass, ConnectionProperty connectionProperty) throws SQLException {
        super(storageClass, connectionProperty);
        HikariConfig hikariConfig = buildConfiguration(getStorageClass().getProperties(), connectionProperty);
        this.dataSource = new HikariDataSource(hikariConfig);
        this.threadCount = connectionProperty.getThreadCount();
    }

    @Override
    public String getStorageVersion() {
        try {
            return getConnection().getMetaData().getDatabaseProductVersion();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getStorageMajorVersion() {
        try {
            return getConnection().getMetaData().getDatabaseMajorVersion();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public S getSession() {
        return (S) getConnection();
    }

    @Override
    public void setSession(S session) {
        setConnection(session);
    }

    @Override
    public S getPoolConnection() throws SQLException {
            return (S) dataSource.getConnection();
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    private HikariConfig buildConfiguration(Properties property, ConnectionProperty connectionProperty) throws SQLException {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(property.getProperty("url"));
        hikariConfig.setUsername(property.getProperty("user"));
        hikariConfig.setPassword(property.getProperty("password"));
        hikariConfig.setMaximumPoolSize(connectionProperty.getThreadCount() + 1);
        hikariConfig.setConnectionTimeout(3_000);
        hikariConfig.setAutoCommit(false);
        return hikariConfig;
    }

    @Override
    public void start(List<Config> cfgs, boolean sync, int rows, Storage<K, T, S, R> targetStorage, String tableName,  Storage<K, T, S, R> cacheStorage) throws SQLException {
        List<Config> configs = copyConfigs(cfgs);
        if (!sync) {
            startNOSync(targetStorage, configs, rows, tableName);
        } else {
            try {
                startSync(targetStorage, configs, rows, tableName);
            } catch (Exception e) {
                log.info("{}", getStackTrace(e));
                targetStorage.closeStorage();
                this.closeStorage();
            }
        }
    }

    @Override
    public List<Config> copyConfigs(List<Config> cfgs) {
        List<Config> configs = new ArrayList<>();
        for (Config c : cfgs) {
            configs.add(c.copy());
        }
        return configs;
    }

    private void startNOSync(Storage<K, T, S, R> targetStorage, List<Config> configs, int rows, String tableName) throws SQLException {
        Connection sourceConnection = this.getPoolConnection();
        setConnection(sourceConnection);

//        Storage<K, T, S, R> sourceStorage = this;
        if (rows > 0) {
            dropChunkTable(configs,false, tableName);
            fulfillChunks(configs, false, rows, tableName);
            targetStorage.dropOutboxTable(false, tableName);
            targetStorage.createGlobalOutbox(tableName);
        }
        log.info("SOURCE version: {}", getStorageMajorVersion());
        sourceConnection.close();

        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        do {
            List<Chunk<K, T, S, R>> chunks = getChunkList(configs, tableName, targetStorage);
            List<Future<Chunk<K, T, S, R>>> futures = new ArrayList<>();
            chunks.forEach(chunk -> futures.add(
                    service
                            .submit(() -> {
                                try {
                                    return chunk.allStages(false, tableName);
                                } catch (Exception e) {
                                    log.error("ChunkId = {} {}.{} {}", chunk.getId(), chunk.getT2t().sourceTable().getSchemaName(), chunk.getT2t().sourceTable().getTableName(), getStackTrace(e));
                                    try {
                                        if ((chunk.getSourceSession()).isValid(0)) {
                                            log.warn("Saving info about error to database");
                                            chunk.interStageSaveChunkStatus(ChunkStatus.PROCESSED_WITH_ERROR, false, null, getStackTrace(e), tableName);
                                            (chunk.getSourceSession()).close();
                                        }
                                        if (targetStorage instanceof  JDBCStorage &&  (chunk.getTargetSession()).isValid(0)) {
                                            (chunk.getTargetSession()).close();
                                        }
                                    } catch (SQLException exception) {
                                        log.error("{}", getStackTrace(exception));
                                    }
                                    throw e;
                                }
                            })
                    )
            );

            int timeoutCounter = 0;
            int errorCounter = 0;
            for (Future<?> future : futures) {
//                Chunk<K, T, S, R> c = null;
                try {
//                    Chunk<K, T, S, R> c = (Chunk<K, T, S, R>) future.get();
                    future.get();
                    Thread.sleep(2);
                } catch (Exception e) {
                    if ((
                                e.getMessage().contains("terminating connection due to administrator command") ||
                                e.getMessage().contains("Database connection failed when ending copy") ||
                                e.getMessage().contains("Write to copy failed") ||
                                e.getMessage().contains("An I/O error occurred while sending to the backend")
                        ) && errorCounter / threadCount < 20) {
                        errorCounter++;
                        log.error("REPEATABLE ISSUE: {}", e.getMessage());
//                        service.shutdownNow();
//                        break;
                    } else if ((
                            e.getMessage().contains("Query timed out after PT2S") ||
                            e.getMessage().contains("Cassandra timeout during BATCH"))
                            && timeoutCounter / threadCount < 20) {
                        try {
                            Thread.sleep(3_000);
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                        timeoutCounter++;
                        log.error("{}", getStackTrace(e));
                    } else {
                        log.error("{}", getStackTrace(e));
                        service.shutdownNow();
                        throw new RuntimeException(e);
                    }
                }
            }

            if (chunks.isEmpty()) {
                log.info("All chunks are processed");
                break;
            }
        } while (true);

        service.shutdown();
        service.close();

        Connection dropChunkConnection = this.getPoolConnection();
        setConnection(dropChunkConnection);
        dropChunkTable(configs, false, tableName);
        dropChunkConnection.close();
        targetStorage.dropOutboxTable(false, tableName);
    }

    @Override
    public boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk, String tableName) {
        return false;
    }

    @Override
    public void insertProcessedChunkInfo(Chunk<?, ?, ?, ?> chunk, String tableName) {
    }

    private void startSync(Storage targetStorage, List<Config> configs, int rows, String tableName) throws SQLException {
        Connection sourceConnection = this.getPoolConnection();
        setConnection(sourceConnection);
        sourceConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

        Storage sourceStorage = this;
        if (rows > 0) {
            fulfillChunks(configs, true, rows, tableName);
            targetStorage.createGlobalOutbox(tableName);
        }
        Map<Table, Table> sourceTables = configsToTables(configs, targetStorage);
        sourceStorage.setTables(sourceTables);
/*
        if (sourceStorage.getClass().equals(targetStorage.getClass())) {
            JDBCStorage sourceJDBCStorage = sourceStorage.unwrap(JDBCStorage.class);
            JDBCStorage targetJDBCStorage = targetStorage.unwrap(JDBCStorage.class);
            log.info("Version source: {}", sourceJDBCStorage.getStorageVersion(sourceConnection));
            if (sourceJDBCStorage.getMajorStorageVersion(sourceConnection) >= 14) {
                sourceJDBCStorage.enrichSourceTables(sourceConnection);
                sourceJDBCStorage.enrichTargetTables();
                targetStorage.setTables(sourceTables);
                targetJDBCStorage.createTables();
            }
        }
*/

        Map.Entry<String,Long> lsnXid = this.getSystemChangeNumberWithTrxId();
        log.info("{} {}", lsnXid.getKey(), lsnXid.getValue());

        List<Chunk<K, T, S, R>> chunks = getChunkList(configs, tableName, targetStorage);
        chunks.forEach(chunk -> {
            try {
                chunk.allStages(true, tableName);
            } catch (Exception e) {
//                log.error("ChunkId = {} {}.{} {}", chunk.getId(), chunk.getT2t().sourceTable().getSchemaName(), chunk.getT2t().sourceTable().getTableName(), getStackTrace(e));
                throw new RuntimeException(e);
            }
        });
        sourceConnection.commit();
        JDBCStorage targetJDBCStorage = targetStorage.unwrap(JDBCStorage.class);
        Connection targetConnection = targetJDBCStorage.getPoolConnection();
        targetJDBCStorage.setTables(getTables());
        targetJDBCStorage.createPrimaryKeys();
        targetJDBCStorage.createUniqueConstraints();
        targetJDBCStorage.createIndexes();
        targetJDBCStorage.createForeignKeys();
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

/*
    @Override
    public void enrichTargetTables() {
        Map<Table<S>, Table<S>> tables = getTables();
        for (Map.Entry<Table<S>, Table<S>> entry : tables.entrySet()) {
            Table<S> sourceTable = entry.getKey();
            Table<S> targetTable = entry.getValue();

            targetTable.setColumns(sourceTable.getColumns());
            targetTable.setPkColumns(sourceTable.getPkColumns());
            targetTable.setIndexes(sourceTable.getIndexes());
            targetTable.setOptions(sourceTable.getOptions());
            targetTable.setUniqueConstraints(sourceTable.getUniqueConstraints());
            targetTable.setForeignKeys(sourceTable.getForeignKeys());
        }
    }
*/

    private boolean inList(List<Table<S>> tables, Table<S> table) {
        return tables.contains(table);
    }

    @Override
    public Table<S> getTagetTableBySourceTable(Table<S> sourceTable) {
        for (Map.Entry<Table<S>, Table<S>> entry : getTables().entrySet()) {
            if (entry.getKey().equals(sourceTable)) {
                return entry.getValue();
            }
        }
        return null;
    }

    @Override
    public Table<S> getSourceTableByTargetTable(Table<S> targetTable) {
        for (Map.Entry<Table<S>, Table<S>> entry : getTables().entrySet()) {
            if (entry.getValue().equals(targetTable)) {
                return entry.getKey();
            }
        }
        return null;
    }

    @Override
    public Map<Table<S>, Table<S>> configsToTables(List<Config> configs, Storage<K, T, S, R> targetStorage) {
        Map<Table<S>, Table<S>> tables = new HashMap<>();
        for (Config c : configs) {
            tables.put(configToTable(c.fromSchemaName(), c.fromTableName()), targetStorage.configToTable(c.toSchemaName(), c.toTableName()));
        }
        return tables;
    }

    @Override
    public <C> C unwrap(Class<C> iface) {
        if (iface.isInstance(this)) {
            return (C) this;
        } else {
            throw new RuntimeException("No object found that implements the interface: " + iface.getName());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    @Override
    public void close() throws Exception {
        closeStorage();
    }

    public boolean isColumnNameWithAsConstruction(String columnName) {
        return columnName.toLowerCase().lastIndexOf(" as ") != -1;
    }

    @Override
    public <V, W> void insertColumnValue(List<ColumnValue<V>> columnValues, Chunk<K, T, S, R> chunk, W writer) throws SQLException {
    }

    @Override
    public <W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) throws SQLException, IOException {
        return null;
    }

    @Override
    public <W> void closeWriter(W writer, Chunk<K, T, S, R> chunk, String tableName) throws SQLException {

    }

    @Override
    public Map.Entry<String, Long> getSystemChangeNumberWithTrxId() throws SQLException {
        return null;
    }

    @Override
    public <W extends Serializable> byte[] intervalYM2Interval(W intervalym) {
        return null;
    }

    @Override
    public <W extends Serializable> byte[] intervalDS2Interval(W intervalds) {
        return null;
    }

    public List<Column2Column> matchColumns(Table<?> sourceTable, Table<?> targetTable) {
        Map<String, Column> targetColumnsMap = targetTable.getColumns().stream()
                .collect(Collectors.toMap(
                        col -> col.columnName().replace("\"", "").toLowerCase(),
                        col -> col,
                        (existing, replacement) -> existing
                ));

        List<Column2Column> matchedPairs = new ArrayList<>();

        for (Column sourceCol : sourceTable.getColumns()) {
            String cleanSourceName = sourceCol.columnName().replace("\"", "").toLowerCase();

            if (targetColumnsMap.containsKey(cleanSourceName)) {
                Column targetCol = targetColumnsMap.get(cleanSourceName);

                matchedPairs.add(new Column2Column(sourceCol, targetCol));
            }
        }

        return matchedPairs;
    }
}
