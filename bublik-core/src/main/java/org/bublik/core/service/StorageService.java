package org.bublik.core.service;

import org.bublik.core.model.*;
import org.bublik.core.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;


import static org.bublik.core.constants.CLassConstants.*;
import static org.bublik.core.util.Utils.getStackTrace;

public interface StorageService {
    Logger log = LoggerFactory.getLogger(StorageService.class);

    void start(List<Config> configs, boolean sync, int rows, Storage targetStorage, String tableName) throws SQLException;
    void createOutbox(String tableName) throws SQLException;
    void dropOutboxTable(boolean sync, String tableName) throws SQLException;
    List<Chunk<?>> getChunkList(List<Config> configs, Connection connection, String chunkTableName) throws SQLException;
//    Map<Integer, Chunk<?>> getChunkMap(List<Config> configs, Connection connection) throws SQLException;
//    Connection getPoolConnection() throws SQLException;
    LogMessage transferToTarget(Chunk<?> chunk, String tableName) throws SQLException;
    void closeStorage();
    String buildFetchStatement(Config config);
    String buildFetchStatement(Config config, Table sourceTable);
    Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?> chunk);
    Map<Table, Table> configsToTables(List<Config> configs, Storage targetStorage);
    Table configToTable(String schemaName, String tableName);
    Table getTagetTableBySourceTable(Table table);
    Table getSourceTableByTargetTable(Table table);

    static Storage getStorage(StorageClass storageClass, Properties properties, ConnectionProperty connectionProperty) throws SQLException {
//        StorageClass storageClass = StorageService.getStorageClass(properties);
        if (storageClass instanceof AutoColseableStorageClass) {
            Properties props = storageClass.getProperties();
            String className = props.getProperty("class");
            if (className == null || className.isEmpty()) {
                throw new NullPointerException();
            } else {
                return StorageService.reflectStorage(className, properties, connectionProperty);
            }
        }
        if (storageClass instanceof JDBCStorageClass) {
            Driver driver = DriverManager.getDriver(properties.getProperty("url"));
            return switch (driver.getClass().getName()) {
                case "oracle.jdbc.OracleDriver" ->
                    StorageService.reflectStorage(ORACLE_STORAGE_CLASS_NAME, properties, connectionProperty);
                case "org.postgresql.Driver" ->
                    StorageService.reflectStorage(POSTGRES_STORAGE_CLASS_NAME, properties, connectionProperty);
                case "tech.ydb.jdbc.YdbDriver" ->
                    StorageService.reflectStorage(YDB_STORAGE_CLASS_NAME, properties, connectionProperty);
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

    static void init(ConnectionProperty property, List<Config> configs, boolean sync, int rows, String chunkTable) throws SQLException {
        log.info("Bublik starting...");
        StorageClass sourceStorageClass = StorageService.getStorageClass(property.getFromProperty());
        StorageClass targetStorageClass = StorageService.getStorageClass(property.getToProperty());
        try (Storage sourceStorage = StorageService.getStorage(sourceStorageClass, property.getFromProperty(), property);
             Storage targetStorage = StorageService.getStorage(targetStorageClass, property.getToProperty(), property)) {
            assert sourceStorage != null;
            sourceStorage.start(configs, sync, rows, targetStorage, chunkTable);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
