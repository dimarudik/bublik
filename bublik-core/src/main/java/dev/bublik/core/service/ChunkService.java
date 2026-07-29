package dev.bublik.core.service;

import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.model.Chunk;
import dev.bublik.core.model.Table;
import dev.bublik.core.storage.Storage;

import java.sql.SQLException;
import java.time.Instant;

public interface ChunkService {
    boolean isValidSourceSession() throws SQLException;
    boolean isValidTargetSession() throws SQLException;
    <R> R getData(String query) throws SQLException;
    Chunk<?, ?, ?, ?> allStages(boolean sync, Table outboxTable) throws SQLException;
    Chunk<?, ?, ?, ?> interStageSaveChunkStatus(ChunkStatus newStatus, boolean sync, Integer errNum, String errMsg, String outboxTable) throws SQLException;
    Chunk<?, ?, ?, ?> secondStageGetSourceResultSet() throws SQLException;
    Chunk<?, ?, ?, ?> mainStageTransfer(String outboxTable) throws SQLException;
    Chunk<?, ?, ?, ?> interStageSaveChunkRows(int copied, boolean sync, String outboxTable) throws SQLException;
    void lastStageCloseSourceSession(boolean sync) throws SQLException;

    default <K, T, S extends AutoCloseable, R> Chunk<K, T, S, R> firstStageAssignSourceSession(Chunk<K, T, S, R> chunk) throws SQLException {
        Storage storage = chunk.getSourceStorage();
        S sourceConnection = storage.getPoolConnection();
        chunk.setSourceSession(sourceConnection);
        return chunk;
    }

    default <K, T, S extends AutoCloseable, R> Chunk<K, T, S, R> firstStageAssignTargetSession(Chunk<K, T, S, R> chunk) throws SQLException {
        Storage storage = chunk.getTargetStorage();
        S targetSession = storage.getPoolConnection();
        chunk.setTargetSession(targetSession);
        chunk.setStartTs(Instant.now());
        return chunk;
    }
}

