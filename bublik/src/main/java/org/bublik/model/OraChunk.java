package org.bublik.model;

import org.bublik.constants.ChunkStatus;
import org.bublik.constants.PGKeywords;
import org.bublik.storage.Storage;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.bublik.constants.SQLConstants.DML_UPDATE_STATUS_ROWID_CHUNKS;

public class OraChunk<T extends RowId> extends Chunk<T> {
    public OraChunk(Integer id, T start, T end, Config config, Table sourceTable, Storage sourceStorage) {
        super(id, start, end, config, sourceTable, sourceStorage);
    }

    @Override
    public OraChunk<T> setChunkStatus(ChunkStatus status, Integer errNum, String errMsg) {
        try {
            Connection connection = this.getSourceConnection();
            CallableStatement callableStatement =
                    connection.prepareCall(DML_UPDATE_STATUS_ROWID_CHUNKS);
            callableStatement.setString(1, this.getConfig().fromTaskName());
            callableStatement.setInt(2, this.getId());
            callableStatement.setInt(3, status.ordinal());
            callableStatement.execute();
            callableStatement.close();
        } catch (SQLException e) {
            throw  new RuntimeException(e);
        }
        return this;
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
    public String buildFetchStatement() {
        List<String> strings = new ArrayList<>();
        Map<String, String> columnToColumnMap = getConfig().columnToColumn();
        Map<String, String> expressionToColumnMap = getConfig().expressionToColumn();
        if (columnToColumnMap != null) {
            strings.addAll(columnToColumnMap.keySet());
        }
        if (expressionToColumnMap != null) {
            strings.addAll(expressionToColumnMap.keySet());
        }
        String columnToColumn = String.join(", ", strings);
        return  PGKeywords.SELECT + " /* bublik */ " +
                (getConfig().fetchHintClause() == null ? "" : getConfig().fetchHintClause()) + " " +
                columnToColumn + " " +
                PGKeywords.FROM + " " +
                getConfig().fromSchemaName() +
                "." +
                getConfig().fromTableName() + " " +
                (getConfig().fromTableNameAdds() == null ? "" : getConfig().fromTableNameAdds()) + " " +
                PGKeywords.WHERE + " " +
                (getConfig().fetchWhereClause() == null ? " " : getConfig().fetchWhereClause() + " and ") +
                " rowid between ? and ?";
    }
}
