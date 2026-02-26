package dev.bublik.core.service;

import dev.bublik.core.model.Config;

import java.sql.SQLException;
import java.util.List;

public interface Source {
    void fulfillChunks(List<Config> configs, boolean sync, int rows, String tableName) throws SQLException;
    void dropChunkTable(boolean sync, String tableName) throws SQLException;
}
