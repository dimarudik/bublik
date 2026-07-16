package dev.bublik.core.service;

import dev.bublik.core.model.Config;

import java.sql.SQLException;
import java.util.List;

public interface Source {
    void fulfillChunks(List<Config> configs, boolean sync, int rows) throws SQLException;
    <S> void preChecks(S session, List<Config> configs) throws SQLException;
    <S> void createChunkTable(S session) throws SQLException;
    void dropChunkTable(List<Config> configs) throws SQLException;
}
