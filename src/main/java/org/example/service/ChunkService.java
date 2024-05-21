package org.example.service;

import org.example.model.Chunk;
import org.example.model.Table;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface ChunkService {
    ThreadLocal<Chunk<?>> CHUNK_THREAD_LOCAL = new ThreadLocal<>();

    void markChunkAsProceed(Connection connection) throws SQLException;
    ResultSet getData(Connection connection, String query) throws SQLException;
    String buildFetchStatement(Map<String, Integer> columnsFromDB);
    Chunk<?> buildChunkWithTargetTable(Chunk<?> chunk, Table targetTable);

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

