package org.example.model;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.example.constants.SQLConstants.DML_UPDATE_STATUS_ROWID_CHUNKS;

@NoArgsConstructor
@Getter
public class OraChunk<T extends String> extends Chunk<T> {
    public OraChunk(Integer id, T start, T end, Config config) {
        super(id, start, end, config);
    }

    @Override
    public void markChunkAsProceed(Connection connection) throws SQLException {
        CallableStatement callableStatement =
                connection.prepareCall(DML_UPDATE_STATUS_ROWID_CHUNKS);
        callableStatement.setString(1, this.getConfig().fromTaskName());
        callableStatement.setInt(2, this.getId());
        callableStatement.execute();
        callableStatement.close();
    }

    @Override
    public ResultSet getData(Connection connection, String query) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, this.getStart());
        statement.setString(2, this.getEnd());
        statement.setFetchSize(10000);
        return statement.executeQuery();
    }

    @Override
    public String buildFetchStatement(Map<String, Integer> columnsFromDB) {
        List<String> neededSourceColumns = new ArrayList<>(columnsFromDB.keySet());
        String expressionToColumn = "";
        if (getConfig().expressionToColumn() != null) {
            expressionToColumn = ", " + String.join(", ", getConfig().expressionToColumn().keySet());
        }
        return "select " +
                getConfig().fetchHintClause() +
                String.join(", ", neededSourceColumns) +
                expressionToColumn +
                " from " +
                getConfig().fromSchemaName() +
                "." +
                getConfig().fromTableName() +
                " where " +
                getConfig().fetchWhereClause() +
                " and rowid between ? and ?";
    }
}
