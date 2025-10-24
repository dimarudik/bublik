package org.bublik.core.service;

import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.model.Chunk;

import java.sql.SQLException;

public interface ChunkService<K, T, S extends AutoCloseable, R> {
    Chunk<?, ?, ?, ?> saveChunkStatus(ChunkStatus status, boolean sync, Integer errNum, String errMsg, String chunkTableName) throws SQLException;
    Chunk<?, ?, ?, ?> saveChunkRows(int rows, boolean sync, String chunkTableName) throws SQLException;
    void closeChunkSourceSession(boolean sync) throws SQLException;
    Chunk<?, ?, ?, R> assignSourceResultSet() throws SQLException;
    R getData(String query) throws SQLException;
    Chunk<K, T, S, R> assignResultLogMessage(String tableName) throws SQLException;
    Chunk<K, T, S, R> assignSourceSession() throws SQLException;
    Chunk<K, T, S, R> assignTargetSession() throws SQLException;
    Chunk<K, T, S, R> copyChunk(boolean sync, String tableName) throws SQLException;

    default Chunk<?, ?, ?, ?> saveChunkStatus(ChunkStatus status, boolean sync, String chunkTableName) throws SQLException {
        return saveChunkStatus(status, sync, null, null, chunkTableName);
    }
}

