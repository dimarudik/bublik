package org.bublik.service;

import org.bublik.model.Chunk;
import org.bublik.model.Table;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface ChunkService {
    ThreadLocal<Chunk<?>> CHUNK_THREAD_LOCAL = new ThreadLocal<>();

    void markChunkAsProceed(Connection connection) throws SQLException;
    ResultSet getData(Connection connection, String query) throws SQLException;
    String buildFetchStatement();
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

