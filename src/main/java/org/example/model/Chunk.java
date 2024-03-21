package org.example.model;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface Chunk {
    Integer chunkId();
    String startRowId();
    String endRowId();
    Long startId();
    Long endId();
    Long startPage();
    Long endPage();
    Config config();
    void markChunkAsProceed(Connection connection) throws SQLException;
    ResultSet getData(Connection connection, String query) throws SQLException;
    Map<String, Integer> readSourceColumns(Connection connection) throws SQLException;
    String buildFetchStatement(Map<String, Integer> columnsFromDB);
}
