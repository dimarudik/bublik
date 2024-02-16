package org.example.service;

import org.example.constants.SourceContextHolder;
import org.example.model.Chunk;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface ChunkService {
    void markChunkAsProceed(Chunk chunk, Connection connection, SourceContextHolder contextHolder) throws SQLException;
    ResultSet getChunkOfData(Chunk chunk, Connection connection, SourceContextHolder contextHolder, String query) throws SQLException;
}
