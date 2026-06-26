package dev.bublik.core.service;

import dev.bublik.core.exception.SourceSQLException;
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
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;


import static dev.bublik.core.constants.CLassConstants.*;
import static dev.bublik.core.util.Utils.getStackTrace;

public interface StorageService {
    Logger log = LoggerFactory.getLogger(StorageService.class);

    void start(Storage targetStorage, List<Config> configs, int rows) throws SQLException;
    void start(Storage targetStorage, List<Config> configs, int rows, String tableName) throws SQLException;
    void start(Storage targetStorage, List<Config> configs, int rows, String tableName, boolean sync) throws SQLException;
    void createGlobalOutbox(String tableName) throws SQLException;
    <K, T, S extends AutoCloseable, R, V, W> void insertColumnValue(List<ColumnValue<V>> columnValues, Chunk<K, T, S, R> chunk, W writer) throws SQLException;
    <K, T, S extends AutoCloseable, R, W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) throws SQLException, SourceSQLException, IOException;
    <K, T, S extends AutoCloseable, R, W> void closeWriter(W writer, Chunk<K, T, S, R> chunk, String tableName) throws SQLException;
    void insertProcessedChunkInfo(Chunk <?, ?, ?, ?> chunk, String tableName) throws SQLException;
    boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk, String tableName) throws SQLException;
    void dropOutboxTable(boolean sync, String tableName) throws SQLException;
    List<Config> copyConfigs(List<Config> cfgs);
    List<Chunk<?, ?, ?, ?>> getChunkList(List<Config> configs, String chunkTableName, Storage targetStorage) throws SQLException;
    String buildStartEndOfChunk(Config config, String chunkTableName, Table sourceTable);
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

    static Storage getStorage(StorageClass storageClass, Properties properties, ConnectionProperty connectionProperty) throws SQLException {
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
//        if (className != null && url == null) {
        if (className != null) {
            return new AutoColseableStorageClass(AutoCloseable.class, properties);
        } else {
            return new JDBCStorageClass(Connection.class, properties);
        }
    }

    static Storage reflectStorage(String className, Properties properties, ConnectionProperty connectionProperty) {
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor(StorageClass.class, ConnectionProperty.class);
            StorageClass storageClass = getStorageClass(properties);
            log.info("Storage class: {} ", className);
            return (Storage) constructor.newInstance(storageClass, connectionProperty);
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
        try (Storage sourceStorage = getStorage(sourceStorageClass, property.getFromProperty(), property);
             Storage targetStorage = getStorage(targetStorageClass, property.getToProperty(), property)) {
            assert sourceStorage != null;
            sourceStorage.start(targetStorage, configs, rows, chunkTable, sync);
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
