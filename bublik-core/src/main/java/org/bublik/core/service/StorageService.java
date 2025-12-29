package org.bublik.core.service;

import org.bublik.core.model.*;
import org.bublik.core.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;


import static org.bublik.core.constants.CLassConstants.*;
import static org.bublik.core.util.Utils.getStackTrace;

public interface StorageService<K, T, S extends AutoCloseable, R> {
    Logger log = LoggerFactory.getLogger(StorageService.class);

    void start(List<Config> configs, boolean sync, int rows, Storage<K, T, S, R> targetStorage, String tableName) throws SQLException;
    void createGlobalOutbox(String tableName) throws SQLException;
    void createLocalOutbox(String tableName) throws SQLException;
    void insertProcessedChunkInfo(Chunk <?, ?, ?, ?> chunk, String tableName) throws SQLException;
    boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk, String tableName) throws SQLException;
    void dropOutboxTable(boolean sync, String tableName) throws SQLException;
    List<Config> copyConfigs(List<Config> cfgs);
    List<Chunk<K, T, S, R>> getChunkList(List<Config> configs, String chunkTableName, Storage<K, T, S, R> targetStorage) throws SQLException;
    String buildStartEndOfChunk(Config config, String chunkTableName);
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
    void setSession(S session);
    void enrichTable(Table<S> sourceTable) throws SQLException;
    void enrichTable(Table<S> sourceTable, Table<S> targetTable) throws SQLException;

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
                case "org.postgresql.Driver" ->
                    reflectStorage(POSTGRES_STORAGE_CLASS_NAME, properties, connectionProperty);
                case "tech.ydb.jdbc.YdbDriver" ->
                    reflectStorage(YDB_STORAGE_CLASS_NAME, properties, connectionProperty);
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
        log.info("version : {}", getVersion());
        log.info("hostname: {}", InetAddress.getLocalHost().getHostName());
        log.info("THREADS: {}", property.getThreadCount());
        String sourceUrl = property.getFromProperty().getProperty("url");
        String sourceHosts = property.getFromProperty().getProperty("hosts");
        log.info("SOURCE: {}", sourceUrl == null ? sourceHosts : sourceUrl);
        log.info("SOURCE USERNAME: {}", property.getFromProperty().getProperty("user"));
        String targetUrl = property.getToProperty().getProperty("url");
        String targetHosts = property.getToProperty().getProperty("hosts");
        log.info("TARGET: {}", targetUrl == null ? targetHosts : targetUrl);
        log.info("TARGET USERNAME: {}", property.getToProperty().getProperty("user"));
        StorageClass sourceStorageClass = StorageService.getStorageClass(property.getFromProperty());
        StorageClass targetStorageClass = StorageService.getStorageClass(property.getToProperty());
        try (Storage sourceStorage = getStorage(sourceStorageClass, property.getFromProperty(), property);
             Storage targetStorage = getStorage(targetStorageClass, property.getToProperty(), property)) {
            assert sourceStorage != null;
            sourceStorage.start(configs, sync, rows, targetStorage, chunkTable);
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
