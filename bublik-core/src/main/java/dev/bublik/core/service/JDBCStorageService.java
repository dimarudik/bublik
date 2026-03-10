package dev.bublik.core.service;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface JDBCStorageService<K, T, S extends AutoCloseable, R> extends StorageService<K, T, S, R> {
    Map.Entry<String,Long> getSystemChangeNumberWithTrxId() throws SQLException;
//    void createTables();
    void createPrimaryKeys();
    void createUniqueConstraints();
    void createIndexes();
    void createForeignKeys();
//    String getStorageVersion(Connection connection) throws SQLException;
//    int getMajorStorageVersion(Connection connection) throws SQLException;
//    void enrichSourceTables(Connection connection);
//    void enrichTargetTables();
    <W extends Serializable> byte[] intervalYM2Interval(W intervalym);
    <W extends Serializable> byte[] intervalDS2Interval(W intervalds);
    Connection getConnection() throws SQLException;
    void setConnection(Connection connection) throws SQLException;
}
