package org.bublik.service;

import org.bublik.constants.ChunkStatus;
import org.bublik.model.Chunk;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface ChunkService {
    ThreadLocal<Chunk<?>> CHUNK_THREAD_LOCAL = new ThreadLocal<>();

    Chunk<?> saveChunkStatus(ChunkStatus status, Integer errNum, String errMsg) throws SQLException;
    Chunk<?> saveChunkRows(int rows) throws SQLException;
    Chunk<?> saveConfig() throws SQLException;
    Chunk<?> assignSourceResultSet() throws SQLException;
    ResultSet getData(Connection connection, String query) throws SQLException;
    void insertProcessedChunkInfo(Connection connection, int rows) throws SQLException;

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

