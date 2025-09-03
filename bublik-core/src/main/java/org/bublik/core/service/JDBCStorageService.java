package org.bublik.core.service;

import org.bublik.core.model.Config;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface JDBCStorageService extends StorageService {
    Map.Entry<String,Long> getSystemChangeNumberWithTrxId() throws SQLException;
    String buildStartEndOfChunk(List<Config> configs);
    void createTables();
    void createPrimaryKeys();
    void createUniqueConstraints();
    void createIndexes();
    void createForeignKeys();
    <T extends Serializable> byte[] intervalYM2Interval(T intervalym);
    <T extends Serializable> byte[] intervalDS2Interval(T intervalds);
}
