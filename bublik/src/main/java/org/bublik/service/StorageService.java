package org.bublik.service;

import org.bublik.model.*;
import org.bublik.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface StorageService {
    Logger log = LoggerFactory.getLogger(StorageService.class);

    void start(List<Config> configs, boolean sync) throws SQLException;
    void sync() throws SQLException;
    Map<Integer, Chunk<?>> getChunkMap(List<Config> configs) throws SQLException;
    Connection getConnection() throws SQLException;
    LogMessage transferToTarget(Chunk<?> chunk) throws SQLException;
    void closeStorage();
    String buildFetchStatement(Config config);
    Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?> chunk);
    Map<Table, Table> configsToTables(List<Config> configs);
    Table configToTable(Config config);
    Table getTagetTableBySourceTable(Table table);
    boolean tableInSourceList(Table table);
    boolean tableInTargetList(Table table);
    void enrichSourceTables();
    void enrichTargetTables(Map<Table, Table> tables);
    void createTables();
    void createPrimaryKeys();
    void createUniqueConstraints();
    void createIndexes();
    void createForeignKeys();

    static Storage getStorage(Properties properties, ConnectionProperty connectionProperty, Boolean isSource) {
        try {
            StorageClass storageClass = StorageService.getStorageClass(properties);
/*
            if (storageClass instanceof CassandraStorageClass) {
                return new CassandraStorage(storageClass, connectionProperty, isSource);
            }
*/
            try {
                if (storageClass instanceof JDBCStorageClass) {
                    Driver driver = DriverManager.getDriver(properties.getProperty("url"));
                    return switch (driver.getClass().getName()) {
                        case "oracle.jdbc.OracleDriver" ->
                                JDBCOracleStorage.getInstance(storageClass, connectionProperty, isSource);
                        case "org.postgresql.Driver" ->
                                JDBCPostgreSQLStorage.getInstance(storageClass, connectionProperty, isSource);
                        case "tech.ydb.jdbc.YdbDriver" ->
                                JDBCYDBStorage.getInstance(storageClass, connectionProperty, isSource);
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
}
