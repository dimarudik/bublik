package org.bublik.service;

import org.bublik.model.Config;

import java.sql.SQLException;
import java.util.List;

public interface JDBCStorageService {
    String buildStartEndOfChunk(List<Config> configs);
    void hook(List<Config> configs) throws SQLException;
}
