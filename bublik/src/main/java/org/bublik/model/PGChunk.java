package org.bublik.model;

import org.bublik.constants.PGKeywords;
import org.bublik.storage.Storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.bublik.constants.SQLConstants.DML_UPDATE_STATUS_CTID_CHUNKS;

public class PGChunk<T extends Long> extends Chunk<T> {
    public PGChunk(Integer id, T start, T end, Config config, Table sourceTable, Table targetTable,
                   Storage sourceStorage, Storage targetStorage) {
        super(id, start, end, config, sourceTable, targetTable, sourceStorage, targetStorage);
    }

    @Override
    public void markChunkAsProceed(Connection connection) throws SQLException {
        PreparedStatement updateStatus = connection.prepareStatement(DML_UPDATE_STATUS_CTID_CHUNKS);
        updateStatus.setLong(1, this.getId());
        updateStatus.setString(2, this.getConfig().fromTaskName());
        int rows = updateStatus.executeUpdate();
        updateStatus.close();
        connection.commit();
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
        String expressionToColumn = "";
        if (getConfig().expressionToColumn() != null) {
            expressionToColumn = ", " + String.join(", ", getConfig().expressionToColumn().keySet());
        }
        return PGKeywords.SELECT + " " +
                String.join(", ", neededSourceColumns) + " " +
                expressionToColumn + " " +
                PGKeywords.FROM + " " +
                getConfig().fromSchemaName() +
                "." +
                getConfig().fromTableName() + " " +
                PGKeywords.WHERE + " " +
                getConfig().fetchWhereClause() +
                " and ctid >= '(" + getStart() + ",1)' and ctid < '(" + getEnd() + ",1)'";
    }

    @Override
    public Chunk<?> buildChunkWithTargetTable(Chunk<?> chunk, Table targetTable) {
        return new PGChunk<>(
                this.getId(),
                this.getStart(),
                this.getEnd(),
                this.getConfig(),
                this.getSourceTable(),
                targetTable,
                getSourceStorage(),
                getTargetStorage()
        );
    }
}
