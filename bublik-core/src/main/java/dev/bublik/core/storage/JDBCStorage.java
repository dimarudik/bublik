package dev.bublik.core.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.bublik.core.model.*;
import dev.bublik.core.service.JDBCStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static dev.bublik.core.constants.Constants.FETCH_SIZE;

public abstract class JDBCStorage extends Storage
        implements JDBCStorageService {
    private static final Logger log = LoggerFactory.getLogger(JDBCStorage.class);
    private final DataSource dataSource;
    private final boolean isManagedPool;
    private final int fetchSize;

    public JDBCStorage(DataSource dataSource, Table outboxTable) {
        super(new ConnectionProperty(), outboxTable);
        this.dataSource = dataSource;
        this.threadCount = getMaxPoolSize(dataSource, 10);
        this.isManagedPool = false;
        this.fetchSize = FETCH_SIZE;
    }

    public JDBCStorage(DataSource dataSource, int threadCount, Table outboxTable) {
        super(new ConnectionProperty(), outboxTable);
        this.dataSource = dataSource;
        this.threadCount = threadCount;
        this.isManagedPool = false;
        this.fetchSize = FETCH_SIZE;
    }

    protected JDBCStorage(DataSource dataSource,
                          ConnectionProperty connectionProperty,
                          Table outboxTable) {
        super(connectionProperty, outboxTable);
        this.dataSource = dataSource;
        this.threadCount = connectionProperty.getThreadCount();
        this.isManagedPool = false;
        this.fetchSize = FETCH_SIZE;
    }

    public JDBCStorage(StorageClass storageClass,
                       ConnectionProperty connectionProperty,
                       Table outboxTable) throws SQLException {
        super(storageClass, connectionProperty, outboxTable);
        HikariConfig hikariConfig = buildConfiguration(storageClass.getProperties(), connectionProperty);
        this.dataSource = new HikariDataSource(hikariConfig);
        this.threadCount = connectionProperty.getThreadCount();
        this.fetchSize = storageClass.getProperties().getProperty("fetchSize") == null ?
                FETCH_SIZE : Integer.parseInt(storageClass.getProperties().getProperty("fetchSize"));
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
            Connection connection = getPoolConnection();
            String version = connection.getMetaData().getDatabaseProductVersion();
            connection.close();
            return version;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getStorageMajorVersion() {
        try {
            Connection connection = getPoolConnection();
            int version = connection.getMetaData().getDatabaseMajorVersion();
            connection.close();
            return version;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <S extends AutoCloseable> S getSession() {
        return null;
    }

    @Override
    public <S extends AutoCloseable> void setSession(S session) {
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

    private HikariConfig buildConfiguration(Properties property, ConnectionProperty connectionProperty) throws SQLException {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(property.getProperty("url"));
        hikariConfig.setUsername(property.getProperty("user"));
        hikariConfig.setPassword(property.getProperty("password"));
        hikariConfig.setMaximumPoolSize(connectionProperty.getThreadCount());
        hikariConfig.setConnectionTimeout(3_000);
        hikariConfig.setAutoCommit(false);
        return hikariConfig;
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

    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public <K, T, S extends AutoCloseable, R> void flushBuffer(Chunk<K, T, S, R> chunk) {

    }
}
