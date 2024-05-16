package org.example.model;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class OraChunk extends Chunk<String> {
    @Override
    public void markChunkAsProceed(Connection connection) throws SQLException {

    }

    @Override
    public ResultSet getData(Connection connection, String query) throws SQLException {
        return null;
    }

    @Override
    public String buildFetchStatement(Map<String, Integer> columnsFromDB) {
        return "";
    }
}
