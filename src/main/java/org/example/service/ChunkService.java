package org.example.service;

import org.example.model.*;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface ChunkService {
    void markChunkAsProceed(Connection connection) throws SQLException;
    ResultSet getData(Connection connection, String query) throws SQLException;
    String buildFetchStatement(Map<String, Integer> columnsFromDB);

    static Chunk getChunk(Connection connection, Integer id, Config config) throws SQLException {
        if (connection.isWrapperFor(oracle.jdbc.OracleConnection.class)) {
            return new OraChunk(id, "", "", config);
        } else if (connection.isWrapperFor(PGConnection.class)) {
            return new PGChunk(id, 1L, 1L, config);
        }
        throw new SQLException("Unknown DataSource");
    }

    static <T> Chunk<T> getChunk(Connection connection, Integer id, T start, T end, Config config) throws Exception {
        if (connection.isWrapperFor(oracle.jdbc.OracleConnection.class)) {
            return (Chunk<T>) new OraChunk(id, (String) start, (String) end, config);
        } else if (connection.isWrapperFor(PGConnection.class)) {
            return (Chunk<T>) new PGChunk();
        }
        return null;
    }
}

