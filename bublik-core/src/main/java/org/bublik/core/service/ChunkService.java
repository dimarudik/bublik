package org.bublik.core.service;

import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.model.Chunk;
import org.bublik.core.model.LogMessage;
import org.bublik.core.storage.Storage;

import java.sql.SQLException;

public interface ChunkService<K, T, S extends AutoCloseable, R> {
    R getData(String query) throws SQLException;
    Chunk<K, T, S, R> allStages(boolean sync, String tableName) throws SQLException;
    Chunk<K, T, S, R> interStageSaveChunkStatus(ChunkStatus newStatus, boolean sync, Integer errNum, String errMsg, String chunkTableName) throws SQLException;
    Chunk<K, T, S, R> secondStageGetSourceResultSet() throws SQLException;
    Chunk<K, T, S, R> mainStageTransfer(String tableName) throws SQLException;
    Chunk<K, T, S, R> interStageSaveChunkRows(int rows, boolean sync, String chunkTableName) throws SQLException;
    void lastStageCloseSourceSession(boolean sync) throws SQLException;

    default Chunk<K, T, S, R> firstStageAssignSourceSession(Chunk<K, T, S, R> chunk) throws SQLException {
        Storage<K, T, S, R> storage = chunk.getSourceStorage();
        S sourceConnection = storage.getPoolConnection();
        chunk.setSourceSession(sourceConnection);
        return chunk;
    }

    default Chunk<K, T, S, R> firstStageAssignTargetSession(Chunk<K, T, S, R> chunk) throws SQLException {
        Storage<K, T, S, R> storage = chunk.getTargetStorage();
        S targetSession = storage.getPoolConnection();
        chunk.setTargetSession(targetSession);
        return chunk;
    }
}

