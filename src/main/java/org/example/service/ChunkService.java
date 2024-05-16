package org.example.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface ChunkService {
    void markChunkAsProceed(Connection connection) throws SQLException;
    ResultSet getData(Connection connection, String query) throws SQLException;
    String buildFetchStatement(Map<String, Integer> columnsFromDB);
}

