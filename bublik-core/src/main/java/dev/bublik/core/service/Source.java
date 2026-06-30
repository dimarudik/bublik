package dev.bublik.core.service;

import dev.bublik.core.model.Config;
import dev.bublik.core.model.Table;

import java.sql.SQLException;
import java.util.List;

public interface Source {
    void fulfillChunks(List<Config> configs, boolean sync, int rows) throws SQLException;
    void dropChunkTable(List<Config> configs, boolean sync) throws SQLException;
}
