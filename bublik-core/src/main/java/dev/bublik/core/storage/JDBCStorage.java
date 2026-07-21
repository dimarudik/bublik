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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static dev.bublik.core.util.Utils.getStackTrace;

public abstract class JDBCStorage extends Storage
        implements JDBCStorageService, Source, Target {
    private static final Logger log = LoggerFactory.getLogger(JDBCStorage.class);
    private final DataSource dataSource;
    private Connection connection;
    private final boolean isManagedPool;

    public JDBCStorage(DataSource dataSource, Table outboxTable) {
        super(new ConnectionProperty(), outboxTable);
        this.dataSource = dataSource;
        this.threadCount = getMaxPoolSize(dataSource, 10);
        this.isManagedPool = false;
    }

    public JDBCStorage(DataSource dataSource, int threadCount, Table outboxTable) {
        super(new ConnectionProperty(), outboxTable);
        this.dataSource = dataSource;
        this.threadCount = threadCount;
        this.isManagedPool = false;
    }

    protected JDBCStorage(DataSource dataSource,
                          ConnectionProperty connectionProperty,
                          Table outboxTable) {
        super(connectionProperty, outboxTable);
        this.dataSource = dataSource;
        this.threadCount = connectionProperty.getThreadCount();
        this.isManagedPool = false;
    }

    public JDBCStorage(StorageClass storageClass,
                       ConnectionProperty connectionProperty,
                       Table outboxTable) throws SQLException {
        super(storageClass, connectionProperty, outboxTable);
        HikariConfig hikariConfig = buildConfiguration(storageClass.getProperties(), connectionProperty);
        this.dataSource = new HikariDataSource(hikariConfig);
        this.threadCount = connectionProperty.getThreadCount();
        this.isManagedPool = true;
    }

    private static int getMaxPoolSize(DataSource dataSource, int defaultValue) {
        if (dataSource == null) {
            return defaultValue;
        }
        if (dataSource instanceof HikariDataSource) {
            return ((HikariDataSource) dataSource).getMaximumPoolSize();
        }
        return defaultValue;
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
    public <S extends AutoCloseable> S getSession() {
        return (S) getConnection();
    }

    @Override
    public <S extends AutoCloseable> void setSession(S session) {
        setConnection((Connection) session);
    }

    @Override
    public <S extends AutoCloseable> S getPoolConnection() throws SQLException {
        Connection conn = dataSource.getConnection();

        if (conn.getAutoCommit()) {
            conn.setAutoCommit(false);
            log.debug("Auto-commit was ENABLED on external DataSource. Forcefully disabled for batch processing.");
        }

        return (S) conn;
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
        hikariConfig.setConnectionTimeout(10_000);
        hikariConfig.setAutoCommit(false);
        return hikariConfig;
    }

    @Override
    public void start(Storage targetStorage, List<Config> configs, int rows) throws SQLException {
        start(targetStorage, configs, rows, false);
    }

    @Override
    public List<Config> copyConfigs(List<Config> cfgs) {
        List<Config> configs = new ArrayList<>();
        for (Config c : cfgs) {
            configs.add(c.copy());
        }
        return configs;
    }

    @Override
    public void start(Storage targetStorage, List<Config> cfgs, int rows, boolean sync) throws SQLException {
        List<Config> configs = copyConfigs(cfgs);
        Connection sourceConnection = this.getPoolConnection();
        setConnection(sourceConnection);

        if (rows > 0) {
            preChecks(connection, configs);
            createChunkTable(connection);
            fulfillChunks(configs, false, rows);
            targetStorage.createGlobalOutbox();
        }
        log.info("SOURCE version: {}", getStorageMajorVersion());
        sourceConnection.close();

        int errorCounter = 0;
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        do {
            List<Chunk<?, ?, ?, ?>> chunks = getChunkList(configs, targetStorage);
            List<Future<Chunk<?, ?, ?, ?>>> futures = new ArrayList<>();
            chunks.forEach(chunk -> futures.add(
                    service
                            .submit(() -> {
                                try {
                                    return chunk.allStages(false, getOutboxTable());
                                } catch (Exception e) {
                                    log.error("ChunkId = {} {}.{} {}", chunk.getId(), chunk.getT2t().sourceTable().getSchemaName(), chunk.getT2t().sourceTable().getTableName(), getStackTrace(e));
                                    try {
                                        log.warn("Saving info about error to database");
                                        chunk.interStageSaveChunkStatus(ChunkStatus.PROCESSED_WITH_ERROR, false, null, getStackTrace(e), getOutboxTable().tableToString());
                                        (chunk.getSourceSession()).close();
                                        if (targetStorage instanceof  JDBCStorage) {
                                            (chunk.getTargetSession()).close();
                                        }
                                    } catch (SQLException exception) {
                                        log.error("Trying to close session: {}", getStackTrace(exception));
                                    }
                                    throw new RuntimeException("ChunkId = " + chunk.getId() + " " + e.getMessage(), e);
                                }
                            })
                    )
            );

            boolean hasBatchErrors = false;
            Throwable lastSubmittedException = null;

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    hasBatchErrors = true;
                    lastSubmittedException = e;
                    errorCounter++;
                }
            }

            if (hasBatchErrors) {
                if (errorCounter < (3 * threadCount)) {
                    log.warn("Batch execution encountered errors. Cooling down for 3 seconds before retry (Current try: {})...", errorCounter);
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    continue;
                } else {
                    log.error("Try: {} Unrecoverable error: {}", errorCounter, getStackTrace(lastSubmittedException));
                    log.info("Finishing due to critical stress failure...");
                    service.shutdownNow();
                    throw new RuntimeException(lastSubmittedException);
                }
            }

            try {
                Thread.sleep(2);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
/*
            for (Future<?> future : futures) {
                try {
                    future.get();
                    Thread.sleep(2);
                } catch (Exception e) {
                    errorCounter++;
                    if (errorCounter < (3 * threadCount)) {
                        log.error("Try: {} Repeatable error: {}", errorCounter, e.getMessage());
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    } else {
                        log.error("Try: {} Unrecoverable error: {}", errorCounter, getStackTrace(e));
                        log.info("Finishing...");
                        service.shutdownNow();
                        throw new RuntimeException(e);
                    }
                }
            }
*/

            if (chunks.isEmpty()) {
                log.info("All chunks are processed");
                break;
            }
        } while (true);

        service.shutdown();
        service.close();

        Connection dropChunkConnection = this.getPoolConnection();
        setConnection(dropChunkConnection);
        dropChunkTable(configs);
        dropChunkConnection.close();
        targetStorage.dropOutboxTable(false);
    }

    @Override
    public boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk) {
        return false;
    }

    @Override
    public void closeStorage() {
        if (dataSource instanceof HikariDataSource hikariDataSource && isManagedPool) {
            hikariDataSource.close();
            log.info("HikariDataSource closed successfully.");
        } else {
            log.warn("DataSource is not an instance of HikariDataSource, cannot close.");
        }

        if (isManagedPool && dataSource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) dataSource).close();
                log.info("Bublik-managed HikariDataSource successfully closed.");
            } catch (Exception e) {
                log.error("Error closing managed HikariDataSource: {}", e.getMessage());
            }
        } else {
            log.debug("DataSource is managed by external system (e.g. Spring). Skipping closure.");
        }
    }

    @Override
    public Table getTargetTableBySourceTable(Table sourceTable) {
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
    public <K, T, S extends AutoCloseable, R, V> void insertColumnValue(List<ColumnValue<V>> columnValues, Chunk<K, T, S, R> chunk) throws SQLException {
    }

    @Override
    public <K, T, S extends AutoCloseable, R, W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        return null;
    }

    @Override
    public <K, T, S extends AutoCloseable, R> void closeWriter(Chunk<K, T, S, R> chunk, String tableName) {

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

    public List<Column2Column> matchColumns(Table sourceTable, Table targetTable) {
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

    @Override
    public <K, T, S extends AutoCloseable, R> void flushBuffer(Chunk<K, T, S, R> chunk) {

    }
}
