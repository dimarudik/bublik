package dev.bublik.core.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.bublik.core.model.*;
import dev.bublik.core.service.JDBCStorageService;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static dev.bublik.core.constants.Constants.FETCH_SIZE;
import static dev.bublik.core.constants.Constants.POOL_SIZE;

public abstract class JDBCStorage extends Storage implements JDBCStorageService {
    private static final System.Logger log = System.getLogger(JDBCStorage.class.getName());
    private final DataSource dataSource;
    private final int fetchSize;
    protected Connection dynamicHeartbeatConnection = null;

    public JDBCStorage(Properties properties,
                       ConnectionProperty connectionProperty,
                       Table outboxTable) throws SQLException {
        super(properties, connectionProperty, outboxTable);
        HikariConfig hikariConfig = buildConfiguration(properties, connectionProperty);
        this.dataSource = new HikariDataSource(hikariConfig);
        this.threadCount = connectionProperty.getThreadCount();
        this.fetchSize = properties.getProperty("fetchSize") == null ?
                FETCH_SIZE : Integer.parseInt(properties.getProperty("fetchSize"));
        this.isManaged = true;
    }

    protected JDBCStorage(Builder<?, ?> builder) {
        super(builder);
        this.dataSource = builder.dataSource;
        this.isManaged = false;
        this.fetchSize = builder.fetchSize <= 0 ? FETCH_SIZE : builder.fetchSize;
        if (threadCount <= 0) {
            this.threadCount = getMaxPoolSize(dataSource, POOL_SIZE);
        }
    }

    protected static abstract class Builder<C extends JDBCStorage, B extends Builder<C, B>> extends Storage.Builder<C, B> {
        private final DataSource dataSource;
        private int fetchSize;

        public Builder(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public B fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return self();
        }

        @Override
        protected void validate() {
            super.validate();
            if (dataSource == null) {
                throw new IllegalStateException("DataSource must not be null for JDBC Storage");
            }
        }
    }

    @Override
    public void validate(Storage targetStorage, List<Config> configs) throws SQLException {}

    @Override
    public List<Chunk<?, ?, ?, ?>> getChunkList(List<TableMigrationContext> contexts,
                                                Storage targetStorage) throws SQLException {
        List<Chunk<?, ?, ?, ?>> chunks = new ArrayList<>();
        for (TableMigrationContext ctx : contexts) {
            log.log(System.Logger.Level.INFO, "Fetch query: {0} {1}", ctx.fetchQuery(), ctx.orderByClause());
            try (Connection connection = this.getPoolConnection();
                 PreparedStatement ps = connection.prepareStatement(ctx.chunkLookupSql())){
                ps.setString(1, ctx.config().fromTaskName());
                try (ResultSet rs = ps.executeQuery()){
                    while (rs.next()) {
                        chunks.add(getChunk(rs, ctx, targetStorage));
                    }
                }
            }
        }
        return chunks;
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
            log.log(System.Logger.Level.DEBUG, "Auto-commit was ENABLED on external DataSource. Forcefully disabled for batch processing.");
        }

        return (S) conn;
    }

    private HikariConfig buildConfiguration(Properties property, ConnectionProperty connectionProperty) throws SQLException {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(property.getProperty("url"));
        hikariConfig.setUsername(property.getProperty("user"));
        hikariConfig.setPassword(property.getProperty("password"));
        hikariConfig.setMaximumPoolSize(connectionProperty.getThreadCount() + 1);
        hikariConfig.setConnectionTimeout(3_000);
        hikariConfig.setAutoCommit(false);
        configureDataSourceProperties(hikariConfig);
        return hikariConfig;
    }

    @Override
    public void configureDataSourceProperties(HikariConfig config) {

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
        if (this.dynamicHeartbeatConnection != null) {
            try {
                if (!this.dynamicHeartbeatConnection.isClosed()) {
                    this.dynamicHeartbeatConnection.close();
                }
            } catch (Exception e) {
                log.log(System.Logger.Level.ERROR, "Error of closing dynamic heartbeat connection: {0}", e.getMessage());
            } finally {
                this.dynamicHeartbeatConnection = null;
            }
        }

        if (dataSource instanceof HikariDataSource hikariDataSource && isManaged) {
            hikariDataSource.close();
            log.log(System.Logger.Level.INFO, "HikariDataSource closed successfully.");
        } else {
            log.log(System.Logger.Level.WARNING, "DataSource is not an instance of HikariDataSource, cannot close.");
        }

        if (isManaged && dataSource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) dataSource).close();
                log.log(System.Logger.Level.INFO, "Bublik-managed HikariDataSource successfully closed.");
            } catch (Exception e) {
                log.log(System.Logger.Level.ERROR, "Error closing managed HikariDataSource: {0}", e.getMessage());
            }
        } else {
            log.log(System.Logger.Level.DEBUG, "DataSource is managed by external system (e.g. Spring). Skipping closure.");
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
