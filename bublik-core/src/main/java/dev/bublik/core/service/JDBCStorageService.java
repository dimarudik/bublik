package dev.bublik.core.service;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface JDBCStorageService extends StorageService {
    Map.Entry<String,Long> getSystemChangeNumberWithTrxId() throws SQLException;
    void createPrimaryKeys();
    void createUniqueConstraints();
    void createIndexes();
    void createForeignKeys();
    <W extends Serializable> byte[] intervalYM2Interval(W intervalym);
    <W extends Serializable> byte[] intervalDS2Interval(W intervalds);
//    void setConnection(Connection connection) throws SQLException;
}
