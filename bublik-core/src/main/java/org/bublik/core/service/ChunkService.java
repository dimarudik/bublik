package org.bublik.core.service;

import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.model.Chunk;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface ChunkService {
    ThreadLocal<Chunk<?>> CHUNK_THREAD_LOCAL = new ThreadLocal<>();

    Chunk<?> saveChunkStatus(ChunkStatus status, boolean sync, Integer errNum, String errMsg, String chunkTableName) throws SQLException;
    Chunk<?> saveChunkRows(int rows, boolean sync, String chunkTableName) throws SQLException;
//    Chunk<?> saveConfig(boolean sync) throws SQLException;
    Chunk<?> assignSourceResultSet() throws SQLException;
    ResultSet getData(Connection connection, String query) throws SQLException;
    void insertProcessedChunkInfo(Connection connection, int rows) throws SQLException;

    default Chunk<?> saveChunkStatus(ChunkStatus status, boolean sync, String chunkTableName) throws SQLException {
        return saveChunkStatus(status, sync, null, null, chunkTableName);
    }

    static void set(Chunk<?> chunk) {
        CHUNK_THREAD_LOCAL.set(chunk);
    }

    static Chunk<?> get() {
        return CHUNK_THREAD_LOCAL.get();
    }

    static void remove() {
        CHUNK_THREAD_LOCAL.remove();
    }
}

