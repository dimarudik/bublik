package org.bublik.core.service;

import org.bublik.core.model.*;
import org.bublik.core.storage.JDBCStorageClass;
import org.bublik.core.storage.Storage;
import org.bublik.core.storage.StorageClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.bublik.core.constants.CLassConstants.*;

public interface StorageService {
    Logger log = LoggerFactory.getLogger(StorageService.class);

    Map.Entry<String,Long> getSystemChangeNumberWithTrxId() throws SQLException;
    void start(List<Config> configs, boolean sync, int rows) throws SQLException;
    void createChunks(List<Config> configs, boolean synz, int rows) throws SQLException;
    void createOutbox() throws SQLException;
    Map<Integer, Chunk<?>> getChunkMap(List<Config> configs) throws SQLException;
    Map<Integer, Chunk<?>> getChunkMap(List<Config> configs, Connection connection) throws SQLException;
    Connection getConnection() throws SQLException;
    LogMessage transferToTarget(Chunk<?> chunk) throws SQLException;
    void closeStorage();
    String buildFetchStatement(Config config);
    Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?> chunk);
    Map<Table, Table> configsToTables(List<Config> configs, Storage targetStorage);
    Table configToTable(String schemaName, String tableName);
    Table getTagetTableBySourceTable(Table table);
    Table getSourceTableByTargetTable(Table table);
    boolean tableInSourceList(Table table);
    boolean tableInTargetList(Table table);
    void enrichSourceTables();
    void enrichTargetTables(Map<Table, Table> tables);
    void createTables();
    void createPrimaryKeys();
    void createUniqueConstraints();
    void createIndexes();
    void createForeignKeys();
    <T extends Serializable> byte[] intervalYM2Interval(T intervalym);
    <T extends Serializable> byte[] intervalDS2Interval(T intervalds);

    static Storage getStorage(Properties properties, ConnectionProperty connectionProperty) {
        try {
            StorageClass storageClass = StorageService.getStorageClass(properties);
            try {
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
            } catch (SQLException e) {
                throw new RuntimeException();
            }
        } catch (SQLException e) {
            throw new RuntimeException();
        }
        return null;
    }

    static StorageClass getStorageClass(Properties properties) throws SQLException {
        String className = properties.getProperty("className");
        if (className != null ) {
            return null;
/*
            return switch (storageType) {
                case "cassandra" -> new CassandraStorageClass(CqlSession.class, properties);
//                case "ydb" -> new YdbTransportImpl.class;
                default -> throw new RuntimeException("Unknown storage type");
            };
*/
        } else {
            Driver driver = DriverManager.getDriver(properties.getProperty("url"));
            return new JDBCStorageClass(Connection.class, properties);
        }
    }

    static Storage reflectStorage(String className, Properties properties, ConnectionProperty connectionProperty) {
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor(StorageClass.class, ConnectionProperty.class);
            return (Storage) constructor.newInstance(getStorageClass(properties), connectionProperty);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
