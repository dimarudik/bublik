package org.example.model;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.example.constants.SQLConstants.DML_UPDATE_STATUS_CTID_CHUNKS;

@NoArgsConstructor
@Getter
public class PGChunk<T extends Long> extends Chunk<T> {
    public PGChunk(Integer id, T start, T end, Config config) {
        super(id, start, end, config);
    }

    @Override
    public void markChunkAsProceed(Connection connection) throws SQLException {
        PreparedStatement updateStatus = connection.prepareStatement(DML_UPDATE_STATUS_CTID_CHUNKS);
        updateStatus.setLong(1, this.getId());
        updateStatus.setString(2, this.getConfig().fromTaskName());
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
                getConfig().fromSchemaName() +
                "." +
                getConfig().fromTableName() +
                " where " +
                getConfig().fetchWhereClause() +
                " and ctid >= '(" + getStart() + ",1)' and ctid < '(" + getEnd() + ",1)'";
    }
}
