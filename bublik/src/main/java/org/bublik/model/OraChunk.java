package org.bublik.model;

import org.bublik.constants.PGKeywords;
import org.bublik.storage.Storage;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.bublik.constants.SQLConstants.DML_UPDATE_STATUS_ROWID_CHUNKS;

public class OraChunk<T extends RowId> extends Chunk<T> {
    public OraChunk(Integer id, T start, T end, Config config, Table sourceTable, Table targetTable, Storage sourceStorage) {
        super(id, start, end, config, sourceTable, targetTable, sourceStorage);
    }

    @Override
    public void markChunkAsProceed(Connection connection) throws SQLException {
        CallableStatement callableStatement =
                connection.prepareCall(DML_UPDATE_STATUS_ROWID_CHUNKS);
        callableStatement.setString(1, this.getConfig().fromTaskName());
        callableStatement.setInt(2, this.getId());
        callableStatement.execute();
        callableStatement.close();
//        connection.commit();
    }

    @Override
    public ResultSet getData(Connection connection, String query) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setRowId(1, this.getStart());
        statement.setRowId(2, this.getEnd());
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
        return  PGKeywords.SELECT + " /* bublik */ " +
                (getConfig().fetchHintClause() == null ? "" : getConfig().fetchHintClause()) + " " +
                String.join(", ", neededSourceColumns) +
                expressionToColumn + " " +
                PGKeywords.FROM + " " +
                getConfig().fromSchemaName() +
                "." +
                getConfig().fromTableName() + " " +
                PGKeywords.WHERE + " " +
                (getConfig().fetchWhereClause() == null ? " " : getConfig().fetchWhereClause() + " and ") +
                " rowid between ? and ?";
    }

    @Override
    public Chunk<?> buildChunkWithTargetTable(Chunk<?> chunk, Table targetTable) {
        return new OraChunk<>(
                this.getId(),
                this.getStart(),
                this.getEnd(),
                this.getConfig(),
                this.getSourceTable(),
                targetTable,
                getSourceStorage()
        );
    }
}
