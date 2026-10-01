package dev.bublik.core.service;

import dev.bublik.core.model.*;
import dev.bublik.core.storage.AutoColseableStorageClass;
import dev.bublik.core.storage.JDBCStorageClass;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import static dev.bublik.core.constants.CLassConstants.*;

public interface StorageService {
    System.Logger log = System.getLogger(StorageService.class.getName());

    void start(Storage targetStorage, List<Config> configs, int rows) throws SQLException;
    void validate(Storage targetStorage, List<Config> configs) throws SQLException;
    void createGlobalOutbox() throws SQLException;
    <K, T, S extends AutoCloseable, R, V> void insertColumnValue(List<ColumnValue<V>> columnValues, Chunk<K, T, S, R> chunk) throws SQLException;
    <K, T, S extends AutoCloseable, R, W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) throws SQLException;
    <K, T, S extends AutoCloseable, R> void closeWriter(Chunk<K, T, S, R> chunk, String tableName);
    <K, T, S extends AutoCloseable, R> void flushBuffer(Chunk<K, T, S, R> chunk);
    void insertProcessedChunkInfo(Chunk <?, ?, ?, ?> chunk) throws SQLException;
    boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk) throws SQLException;
    void dropOutboxTable(boolean sync) throws SQLException;
    List<Config> copyConfigs(List<Config> cfgs);
    Chunk<?, ?, ?, ?> getChunk(ResultSet rs, TableMigrationContext ctx, Storage targetStorage) throws SQLException;
    List<Chunk<?, ?, ?, ?>> getChunkList(List<TableMigrationContext> migrationContexts, Storage targetStorage) throws SQLException;
    String buildStartEndOfChunk(Config config, Table sourceTable);
    <K, T, S extends AutoCloseable, R> LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException;
    void closeStorage();
    String buildFetchStatement(Config config, Table2Table t2t);
    Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?, ?, ?> chunk);
    Map<Table, Table> configsToTables(List<Config> configs, Storage targetStorage);
    Table configToTable(String schemaName, String tableName);
    Table getTargetTableBySourceTable(Table table);
    Table getSourceTableByTargetTable(Table table);
    <S extends AutoCloseable> S getPoolConnection() throws SQLException;
    <S extends AutoCloseable> S getSession();
    String getStorageVersion();
    int getStorageMajorVersion();
    <S extends AutoCloseable> void setSession(S session);
    void enrichTable(Table sourceTable) throws SQLException;
    void enrichTable(Table sourceTable, Table targetTable) throws SQLException;
    List<Column2Column> getColumn2Column(Table sourceTable, Table targetTable, Config config);
    Table2Table getTable2Table(Table sourceTable, Table targetTable, List<Column2Column> c2c, Config config);
    Column columnFromAvro(Map<String, Object> avroSchema, String avroFieldName, int position);
    int getFetchSize();
    Table getDefaultSourceOutboxTable();
    Table getDefaultTargetOutboxTable();
    boolean tryDistributedLock(long lockId) throws SQLException;
    void releaseDistributedLock(long lockId) throws SQLException;
    void holdInstanceSignalLock() throws SQLException;

    static Storage getStorage(StorageClass storageClass,
                              Properties properties,
                              ConnectionProperty connectionProperty,
                              Table outboxTable) {
        if (storageClass instanceof AutoColseableStorageClass) {
            Properties props = storageClass.getProperties();
            String className = props.getProperty("class");
            if (className == null || className.isEmpty()) {
                throw new NullPointerException();
            } else {
                return reflectStorage(className, properties, connectionProperty, outboxTable);
            }
        }
        if (storageClass instanceof JDBCStorageClass) {
            try {
                Driver driver = DriverManager.getDriver(properties.getProperty("url"));
                return switch (driver.getClass().getName()) {
                    case "oracle.jdbc.OracleDriver" ->
                            reflectStorage(ORACLE_STORAGE_CLASS_NAME, properties, connectionProperty, outboxTable);
                    case "org.postgresql.Driver", "sdk.humus.HumusDriver" ->
                            reflectStorage(POSTGRES_STORAGE_CLASS_NAME, properties, connectionProperty, outboxTable);
                    case "tech.ydb.jdbc.YdbDriver" ->
                            reflectStorage(YDB_STORAGE_CLASS_NAME, properties, connectionProperty, outboxTable);
                    case "com.microsoft.sqlserver.jdbc.SQLServerDriver" ->
                            reflectStorage(MSSQL_STORAGE_CLASS_NAME, properties, connectionProperty, outboxTable);
                    default -> throw new RuntimeException();
                };
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException("Unknown storage class");
    }

    static StorageClass getStorageClass(Properties properties) {
        String className = properties.getProperty("class");
        if (className != null) {
            return new AutoColseableStorageClass(AutoCloseable.class, properties);
        } else {
            return new JDBCStorageClass(Connection.class, properties);
        }
    }

    static Storage reflectStorage(String className,
                                  Properties properties,
                                  ConnectionProperty connectionProperty,
                                  Table outboxTable) {
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor(StorageClass.class,
                    ConnectionProperty.class, Table.class);
            StorageClass storageClass = getStorageClass(properties);
            log.log(System.Logger.Level.INFO, "Storage class: {0} ", className);
            return (Storage) constructor.newInstance(storageClass, connectionProperty, outboxTable);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Deprecated
    static void init(ConnectionProperty property, List<Config> configs, boolean sync, int rows, String table) throws SQLException, IOException {
        throw new RuntimeException("Deprecated");
    }

    static void init(ConnectionProperty property, List<Config> configs, int rows) throws SQLException, IOException {
        init(property, configs, rows, null);
    }

    static void init(ConnectionProperty property, List<Config> configs, int rows, Table chunkTable) throws SQLException, IOException {
        init(property, configs, rows, chunkTable, null);
    }

    static void init(ConnectionProperty property, List<Config> configs, int rows, Table chunkTable, Table outboxTable) throws SQLException, IOException {
        log.log(System.Logger.Level.INFO, "Bublik starting...");
        if (rows > 0) log.log(System.Logger.Level.INFO, "Expected chunk size: {0}", rows);
        log.log(System.Logger.Level.INFO, "VERSION : {0}", getVersion());
        try {
            log.log(System.Logger.Level.INFO, "WORKSTATION: {0}", InetAddress.getLocalHost().getHostName());
        } catch (Exception e) {
            log.log(System.Logger.Level.WARNING, "Unknown workstation");
        }
        Runtime runtime = Runtime.getRuntime();
        int vCPU = runtime.availableProcessors();
        log.log(System.Logger.Level.INFO, "===================== CPU INFO ============================");
        log.log(System.Logger.Level.INFO, "CPU onboard: {0}", vCPU);
        if (vCPU <= 4 && vCPU < property.getThreadCount()) {
            property.setThreadCount(vCPU);
            log.log(System.Logger.Level.INFO, "Thread count throttled to {0}", property.getThreadCount());
        }
        long byteToMb = 1024L * 1024L;
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        log.log(System.Logger.Level.INFO, "=================== MEMORY INFO =========================");
        log.log(System.Logger.Level.INFO, "Max Heap Size (-Xmx):   {0} MB", maxMemory == Long.MAX_VALUE ? "Unlimited" : maxMemory / byteToMb);
        log.log(System.Logger.Level.INFO, "Allocated Heap Size:    {0} MB", totalMemory / byteToMb);
        log.log(System.Logger.Level.INFO, "Used Heap Memory:       {0} MB", usedMemory / byteToMb);
        log.log(System.Logger.Level.INFO, "Free Heap Memory:       {0} MB", (maxMemory - usedMemory) / byteToMb);
        log.log(System.Logger.Level.INFO, "==========================================================");

        String sourceUrl = property.getFromProperty().getProperty("url");
        String sourceHosts = property.getFromProperty().getProperty("hosts");
        log.log(System.Logger.Level.INFO, "SOURCE: {0}", sourceUrl == null ? sourceHosts : sourceUrl);
        log.log(System.Logger.Level.INFO, "SOURCE USERNAME: {0}", property.getFromProperty().getProperty("user"));
        String targetUrl = property.getToProperty().getProperty("url");
        String targetHosts = property.getToProperty().getProperty("hosts");
        log.log(System.Logger.Level.INFO, "TARGET: {0}", targetUrl == null ? targetHosts : targetUrl);
        log.log(System.Logger.Level.INFO, "TARGET USERNAME: {0}", property.getToProperty().getProperty("user"));

        ServiceLoader<StorageFactory> loader = ServiceLoader.load(StorageFactory.class);
        for (StorageFactory factory : loader) {
            log.log(System.Logger.Level.INFO, "Storage factory: {0}", factory.getClass().getName());
//            log.info("Storage factory: {}", factory.getClass().getName());
//            Storage<?, ?, ?, ?> storage = factory.create(property);
//            log.info("Storage: {}", storage.getClass().getName());
        }
/*
        for (Storage<?,?,?,?> storage : loader) {
            storages.add(storage);
        }
        storages.forEach(s -> log.info("Storage: {}", s.getClass().getName()));
*/

/*
        for (ProxyPluginFactory factory : loader) {
            ProxyPlugin plugin = factory.create(url, info);
            if (plugin != null) {
                plugins.add(plugin);
            }
        }
*/
        StorageClass sourceStorageClass = StorageService.getStorageClass(property.getFromProperty());
        StorageClass targetStorageClass = StorageService.getStorageClass(property.getToProperty());
        try (Storage sourceStorage = getStorage(sourceStorageClass, property.getFromProperty(), property, chunkTable);
             Storage targetStorage = getStorage(targetStorageClass, property.getToProperty(), property, outboxTable)) {
            assert sourceStorage != null;
            sourceStorage.start(targetStorage, configs, rows);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static Storage getSourceStorage(ConnectionProperty property, Table chunkTable) {
        Properties properties = property.getFromProperty();
        StorageClass sourceStorageClass = StorageService.getStorageClass(properties);
        return getStorage(sourceStorageClass, properties, property, chunkTable);
    }

    static Storage getTargetStorage(ConnectionProperty property, Table outboxTable) {
        Properties properties = property.getToProperty();
        StorageClass targetStorageClass = StorageService.getStorageClass(properties);
        return getStorage(targetStorageClass, properties, property, outboxTable);
    }

    static String getVersion() throws IOException {
        try {
            final Properties properties = new Properties();
            properties.load(StorageService.class.getClassLoader().getResourceAsStream("project.properties"));
            return properties.getProperty("version");
        } catch (NullPointerException npe) {
            return "unknown";
        }
    }
}
