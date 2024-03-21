package org.example.model;

import java.sql.*;
import java.util.*;

import static org.example.constants.SQLConstants.DML_UPDATE_STATUS_ROWID_CHUNKS;

public record OraChunk(Integer chunkId,
                       String startRowId,
                       String endRowId,
                       Long startId,
                       Long endId,
                       Config config) implements Chunk {
    @Override
    public Long startPage() {
        return null;
    }

    @Override
    public Long endPage() {
        return null;
    }

    @Override
    public void markChunkAsProceed(Connection connection) throws SQLException {
        CallableStatement callableStatement =
                connection.prepareCall(DML_UPDATE_STATUS_ROWID_CHUNKS);
        callableStatement.setString(1, this.config().fromTaskName());
        callableStatement.setInt(2, this.chunkId());
        callableStatement.execute();
        callableStatement.close();
    }

    @Override
    public ResultSet getData(Connection connection, String query) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(query);
        if (this.config().numberColumn() == null) {
            statement.setString(1, this.startRowId());
            statement.setString(2, this.endRowId());
        } else {
            statement.setLong(1, this.startId());
            statement.setLong(2, this.endId());
        }
        return statement.executeQuery();
    }

    @Override
    public Map<String, Integer> readSourceColumns(Connection connection) throws SQLException {
        Map<String, Integer> columnMap = new TreeMap<>();
        ResultSet resultSet;
        resultSet = connection.getMetaData().getColumns(
                null,
                config.fromSchemaName(),
                config.fromTableName().replaceAll("^\"|\"$", ""),
                null
        );
        while (resultSet.next()) {
            String columnName = resultSet.getString(4);
            Integer columnType = Integer.valueOf(resultSet.getString(5));
            columnMap.put(columnName, columnType);
        }
        resultSet.close();
        return columnMap;
    }

    @Override
    public String buildSQLFetchStatement(Map<String, Integer> columnsFromDB) {
        List<String> neededSourceColumns = new ArrayList<>(columnsFromDB.keySet());
        if (config.excludedSourceColumns() != null) {
            Set<String> excludedSourceColumns = new HashSet<>(config.excludedSourceColumns());
            Map<String, Integer> neededSourceColumnsMap = new TreeMap<>(columnsFromDB);
            neededSourceColumnsMap.keySet().removeAll(excludedSourceColumns);
            neededSourceColumns = new ArrayList<>(neededSourceColumnsMap.keySet());
        }
        if (config.numberColumn() == null) {
            return "select " +
                    config.fetchHintClause() +
                    String.join(", ", neededSourceColumns) +
                    " from " +
                    config.fromSchemaName() +
                    "." +
                    config.fromTableName() +
                    " where " +
                    config.fetchWhereClause() +
                    " and rowid between ? and ?";
        } else {
            return "select " +
                    config.fetchHintClause() +
                    String.join(", ", neededSourceColumns) +
                    " from " +
                    config.fromSchemaName() +
                    "." +
                    config.fromTableName() +
                    " where " +
                    config.fetchWhereClause() +
                    " and " + config.numberColumn() + " between ? and ?";
        }
    }
}
