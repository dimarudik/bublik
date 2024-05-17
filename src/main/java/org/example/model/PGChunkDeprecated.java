package org.example.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.example.constants.SQLConstants.DML_UPDATE_STATUS_CTID_CHUNKS;

@Deprecated
public record PGChunkDeprecated(Integer chunkId,
                                Long startPage,
                                Long endPage,
                                Config config) implements ChunkDeprecated {

    @Override
    public String startRowId() {
        return this.startPage().toString();
    }

    @Override
    public String endRowId() {
        return this.endPage().toString();
    }

    @Override
    public Long startId() {
        return null;
    }

    @Override
    public Long endId() {
        return null;
    }

    @Override
    public void markChunkAsProceed(Connection connection) throws SQLException {
        PreparedStatement updateStatus = connection.prepareStatement(DML_UPDATE_STATUS_CTID_CHUNKS);
        updateStatus.setLong(1, this.chunkId());
        updateStatus.setString(2, this.config().fromTaskName());
        int rows = updateStatus.executeUpdate();
        updateStatus.close();
    }

    @Override
    public ResultSet getData(Connection connection, String query) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setFetchSize(10000);
        return preparedStatement.executeQuery();
    }

    @Override
    public String buildFetchStatement(Map<String, Integer> columnsFromDB) {
        List<String> neededSourceColumns = new ArrayList<>(columnsFromDB.keySet());
        return "select " +
                String.join(", ", neededSourceColumns) +
                " from " +
                config.fromSchemaName() +
                "." +
                config.fromTableName() +
                " where " +
                config.fetchWhereClause() +
                " and ctid >= '(" + startPage() + ",1)' and ctid < '(" + endPage() + ",1)'";
    }
}
