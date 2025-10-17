package org.bublik.core.service;

import org.bublik.core.model.Config;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface JDBCStorageService extends StorageService {
    Map.Entry<String,Long> getSystemChangeNumberWithTrxId() throws SQLException;
    String buildStartEndOfChunk(List<Config> configs, String chunkTableName);
    Connection getPoolConnection() throws SQLException;
    void createTables();
    void createPrimaryKeys();
    void createUniqueConstraints();
    void createIndexes();
    void createForeignKeys();
    String getStorageVersion(Connection connection) throws SQLException;
    int getMajorStorageVersion(Connection connection) throws SQLException;
    void enrichSourceTables(Connection connection);
    void enrichTargetTables();
    <T extends Serializable> byte[] intervalYM2Interval(T intervalym);
    <T extends Serializable> byte[] intervalDS2Interval(T intervalds);
    Connection getConnection() throws SQLException;
    void setConnection(Connection connection) throws SQLException;
    void insertProcessedChunkInfo(Connection connection, int chunkId, int rows, String taskName, String tableName) throws SQLException;
}
