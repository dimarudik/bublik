package dev.bublik.core.service;

import dev.bublik.core.cache.CacheHolder;
import dev.bublik.core.exception.SourceSQLException;
import dev.bublik.core.model.*;
import dev.bublik.core.cache.CacheHolder;
import dev.bublik.core.model.Chunk;
import dev.bublik.core.model.Column;
import dev.bublik.core.model.ColumnValue;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.LogMessage;
import dev.bublik.core.model.Table;
import dev.bublik.core.model.Table2Table;
import dev.bublik.core.storage.AutoColseableStorageClass;
import dev.bublik.core.storage.JDBCStorageClass;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.*;

import static dev.bublik.core.constants.CLassConstants.MSSQL_STORAGE_CLASS_NAME;
import static dev.bublik.core.constants.CLassConstants.ORACLE_STORAGE_CLASS_NAME;
import static dev.bublik.core.constants.CLassConstants.POSTGRES_STORAGE_CLASS_NAME;
import static dev.bublik.core.constants.CLassConstants.YDB_STORAGE_CLASS_NAME;
import static dev.bublik.core.util.Utils.getStackTrace;

public interface StorageService<K, T, S extends AutoCloseable, R> {
    Logger log = LoggerFactory.getLogger(StorageService.class);

    void start(List<Config> configs, boolean sync, int rows, Storage<K, T, S, R> targetStorage, String tableName,  Storage<K, T, S, R> cacheStorage) throws SQLException;
    void createGlobalOutbox(String tableName) throws SQLException;
    <V, W> void insertColumnValue(List<ColumnValue<V>> columnValues, Chunk<K, T, S, R> chunk, W writer) throws SQLException;
    <W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) throws SQLException, SourceSQLException, IOException;
    <W> void closeWriter(W writer, Chunk<K, T, S, R> chunk, String tableName) throws SQLException;
    void insertProcessedChunkInfo(Chunk <?, ?, ?, ?> chunk, String tableName) throws SQLException;
    boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk, String tableName) throws SQLException;
    void dropOutboxTable(boolean sync, String tableName) throws SQLException;
    List<Config> copyConfigs(List<Config> cfgs);
    List<Chunk<K, T, S, R>> getChunkList(List<Config> configs, String chunkTableName, Storage<K, T, S, R> targetStorage) throws SQLException;
    String buildStartEndOfChunk(Config config, String chunkTableName, Table<S> sourceTable);
    LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException;
    void closeStorage();
    String buildFetchStatement(Config config, Table2Table<S> t2t);
    Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?, ?, ?> chunk);
    Map<Table<S>, Table<S>> configsToTables(List<Config> configs, Storage<K, T, S, R> targetStorage);
    Table<S> configToTable(String schemaName, String tableName);
    Table<S> getTagetTableBySourceTable(Table<S> table);
    Table<S> getSourceTableByTargetTable(Table<S> table);
    S getPoolConnection() throws SQLException;
    S getSession();
    String getStorageVersion();
    int getStorageMajorVersion();
    void setSession(S session);
    void enrichTable(Table<S> sourceTable) throws SQLException;
    void enrichTable(Table<S> sourceTable, Table<S> targetTable) throws SQLException;
    List<Column2Column> getColumn2Column(Table<S> sourceTable, Table<S> targetTable, Config config);
    Table2Table<S> getTable2Table(Table<S> sourceTable, Table<S> targetTable, List<Column2Column> c2c, Config config);
    void initCache(List<Config> configs) throws SQLException;

    static Storage<?, ?, ?, ?> getStorage(StorageClass storageClass, Properties properties, ConnectionProperty connectionProperty) throws SQLException {
        if (storageClass instanceof AutoColseableStorageClass) {
            Properties props = storageClass.getProperties();
            String className = props.getProperty("class");
            if (className == null || className.isEmpty()) {
                throw new NullPointerException();
            } else {
                return reflectStorage(className, properties, connectionProperty);
            }
        }
        if (storageClass instanceof JDBCStorageClass) {
            Driver driver = DriverManager.getDriver(properties.getProperty("url"));
            return switch (driver.getClass().getName()) {
                case "oracle.jdbc.OracleDriver" ->
                    reflectStorage(ORACLE_STORAGE_CLASS_NAME, properties, connectionProperty);
                case "org.postgresql.Driver", "sdk.humus.HumusDriver" ->
                    reflectStorage(POSTGRES_STORAGE_CLASS_NAME, properties, connectionProperty);
                case "tech.ydb.jdbc.YdbDriver" ->
                    reflectStorage(YDB_STORAGE_CLASS_NAME, properties, connectionProperty);
                case "com.microsoft.sqlserver.jdbc.SQLServerDriver" ->
                    reflectStorage(MSSQL_STORAGE_CLASS_NAME, properties, connectionProperty);
                default -> throw new RuntimeException();
            };
        }
        return null;
    }

    static StorageClass getStorageClass(Properties properties) throws SQLException {
        String className = properties.getProperty("class");
        String url = properties.getProperty("url");
        if (className != null && url == null) {
            return new AutoColseableStorageClass(AutoCloseable.class, properties);
        } else {
            return new JDBCStorageClass(Connection.class, properties);
        }
    }

    static Storage<?, ?, ?, ?> reflectStorage(String className, Properties properties, ConnectionProperty connectionProperty) {
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor(StorageClass.class, ConnectionProperty.class);
            StorageClass storageClass = getStorageClass(properties);
            log.info("Storage class: {} ", className);
            return (Storage<?, ?, ?, ?>) constructor.newInstance(storageClass, connectionProperty);
        } catch (Exception e) {
            log.error("{}", getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    static void init(ConnectionProperty property, List<Config> configs, boolean sync, int rows, String chunkTable) throws SQLException, IOException {
        log.info("Bublik starting...");
        log.info("VERSION : {}", getVersion());
        try {
            log.info("WORKSTATION: {}", InetAddress.getLocalHost().getHostName());
        } catch (Exception e) {
            log.info("Unknown workstation");
        }
        log.info("THREADS: {}", property.getThreadCount());
        String sourceUrl = property.getFromProperty().getProperty("url");
        String sourceHosts = property.getFromProperty().getProperty("hosts");
        log.info("SOURCE: {}", sourceUrl == null ? sourceHosts : sourceUrl);
        log.info("SOURCE USERNAME: {}", property.getFromProperty().getProperty("user"));
        String targetUrl = property.getToProperty().getProperty("url");
        String targetHosts = property.getToProperty().getProperty("hosts");
        log.info("TARGET: {}", targetUrl == null ? targetHosts : targetUrl);
        log.info("TARGET USERNAME: {}", property.getToProperty().getProperty("user"));
        String cacheUrl = property.getCacheProperties() != null ? property.getCacheProperty().getProperty("url") : null;
        String cacheHosts = property.getCacheProperties() != null ? property.getCacheProperty().getProperty("hosts") : null;
        log.info("CACHE: {}", cacheUrl == null ? cacheHosts : cacheUrl);
        log.info("CACHE USERNAME: {}", property.getCacheProperties() != null ? property.getCacheProperty().getProperty("user") : null);

//        List<Storage<?,?,?,?>> storages = new ArrayList<>();
        ServiceLoader<StorageFactory> loader = ServiceLoader.load(StorageFactory.class);
        for (StorageFactory factory : loader) {
            log.info("Storage factory: {}", factory.getClass().getName());
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
        try (Storage<?,?,?,?> sourceStorage = getStorage(sourceStorageClass, property.getFromProperty(), property);
             Storage targetStorage = getStorage(targetStorageClass, property.getToProperty(), property)) {
            assert sourceStorage != null;

            if (property.getCacheProperties() != null) {
                StorageClass cacheStorageClass = StorageService.getStorageClass(property.getCacheProperty());
                try(Storage cacheStorage = getStorage(cacheStorageClass, property.getCacheProperty(), property)) {
                    if (cacheStorage != null) {
                        cacheStorage.initCache(configs);
                        sourceStorage.start(configs, sync, rows, targetStorage, chunkTable, cacheStorage);
                        CacheHolder.clearAll();
                        return;
                    }
                }
            }

            sourceStorage.start(configs, sync, rows, targetStorage, chunkTable, null);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
