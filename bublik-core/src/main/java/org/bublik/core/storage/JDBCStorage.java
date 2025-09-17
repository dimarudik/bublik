package org.bublik.core.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.model.Chunk;
import org.bublik.core.model.Config;
import org.bublik.core.model.ConnectionProperty;
import org.bublik.core.model.Table;
import org.bublik.core.service.JDBCStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.bublik.core.util.Utils.getStackTrace;

public abstract class JDBCStorage extends Storage implements JDBCStorageService {
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
    public String getStorageVersion(Connection connection) throws SQLException {
        return connection.getMetaData().getDatabaseProductVersion();
    }

    @Override
    public int getMajorStorageVersion(Connection connection) throws SQLException {
        return connection.getMetaData().getDatabaseMajorVersion();
    }

    @Override
    public Connection getConnection() throws SQLException {
            return dataSource.getConnection();
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
        return configs
                .stream()
                .filter(config -> config.fromTaskName().equals(taskName))
                .findFirst()
                .orElseThrow();
    }

    @Override
    public void start(List<Config> cfgs, boolean sync, int rows, Storage targetStorage) throws SQLException {
        List<Config> configs = copyConfigs(cfgs);
        if (!sync) {
            startNOSync(targetStorage, configs, rows);
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

    private List<Config> copyConfigs(List<Config> cfgs) {
        List<Config> configs = new ArrayList<>();
        for (Config c : cfgs) {
            configs.add(c.copy());
        }
        return configs;
    }

    private void startNOSync(Storage targetStorage, List<Config> configs, int rows) throws SQLException {
        Connection sourceConnection = this.getConnection();

        Storage sourceStorage = this;
        if (rows > 0) {
            createChunks(sourceConnection, configs, false, rows);
            targetStorage.createOutbox();
        }
        Map<Table, Table> sourceTables = configsToTables(configs, targetStorage);
        sourceStorage.setTables(sourceTables);
        targetStorage.setTables(sourceTables);
        if (sourceStorage.getClass().equals(targetStorage.getClass())) {
            JDBCStorage sourceJDBCStorage = sourceStorage.unwrap(JDBCStorage.class);
            JDBCStorage targetJDBCStorage = targetStorage.unwrap(JDBCStorage.class);
            log.info("Source Version: {} Major Version: {}", sourceJDBCStorage.getStorageVersion(sourceConnection), sourceJDBCStorage.getMajorStorageVersion(sourceConnection));
            sourceJDBCStorage.enrichSourceTables(sourceConnection);
            log.info("{} {}", sourceStorage.getTables().hashCode(), sourceJDBCStorage.getTables().hashCode());
            sourceJDBCStorage.enrichTargetTables();
            targetJDBCStorage.createTables();
        }

//        List<Chunk<?>> chunks = getChunkList(configs, sourceConnection);
        sourceConnection.close();


        ExecutorService service = Executors.newFixedThreadPool(threadCount);


        do {
            Connection sConnection = this.getConnection();
            List<Chunk<?>> chunks = getChunkList(configs, sConnection);
            sConnection.close();
            List<Future<Chunk<?>>> futures = new ArrayList<>();

            chunks.forEach(chunk -> futures.add(
                            service
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
                    )
            );

            for (Future<?> future : futures) {
                try {
                    Chunk<?> c = (Chunk<?>) future.get();
                    Thread.sleep(2);
                } catch (Exception e) {
                    log.error("{}", getStackTrace(e));
                    service.shutdownNow();
                    throw new RuntimeException(e);
                }
            }

            if (chunks.isEmpty()) {
                log.info("Portion of chunks processed");
                break;
            }
        } while (true);

        service.shutdown();
        service.close();
    }

    private void startSync(Storage targetStorage, List<Config> configs, int rows) throws SQLException {
        Connection sourceConnection = this.getConnection();
        sourceConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

        Storage sourceStorage = this;
        if (rows > 0) {
            createChunks(sourceConnection, configs, true, rows);
            targetStorage.createOutbox();
        }
        Map<Table, Table> sourceTables = configsToTables(configs, targetStorage);
        sourceStorage.setTables(sourceTables);
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

        Map.Entry<String,Long> lsnXid = this.getSystemChangeNumberWithTrxId();
        log.info("{} {}", lsnXid.getKey(), lsnXid.getValue());

        List<Chunk<?>> chunks = getChunkList(configs, sourceConnection);
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
        JDBCStorage targetJDBCStorage = targetStorage.unwrap(JDBCStorage.class);
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

    @Override
    public void enrichTargetTables() {
        Map<Table, Table> tables = getTables();
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

    @Override
    public <T> T unwrap(Class<T> iface) {
        if (iface.isInstance(this)) {
            return (T) this;
        } else {
            throw new RuntimeException("No object found that implements the interface: " + iface.getName());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
