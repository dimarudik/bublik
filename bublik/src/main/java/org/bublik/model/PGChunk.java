package org.bublik.model;

import org.bublik.constants.ChunkStatus;
import org.bublik.constants.PGKeywords;
import org.bublik.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.bublik.constants.SQLConstants.PLSQL_UPDATE_STATUS_CTID_CHUNKS;
import static org.bublik.constants.SQLConstants.DML_UPDATE_STATUS_CTID_CHUNKS_WITH_ERRORS;
import static org.bublik.exception.Utils.getStackTrace;

public class PGChunk<T extends Long> extends Chunk<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PGChunk.class);
    public PGChunk(Integer id, T start, T end, Config config, Table sourceTable, Storage sourceStorage) {
        super(id, start, end, config, sourceTable, sourceStorage);
    }

    @Override
    public PGChunk<T> setChunkStatus(ChunkStatus status, Integer errNum, String errMsg) throws SQLException {
        Connection connection = this.getSourceConnection();
        PreparedStatement updateStatus;
        if (errMsg == null) {
            updateStatus = connection.prepareStatement(PLSQL_UPDATE_STATUS_CTID_CHUNKS);
            updateStatus.setString(1, status.toString());
            updateStatus.setLong(2, this.getId());
            updateStatus.setString(3, this.getConfig().fromTaskName());
        } else {
            updateStatus = connection.prepareStatement(DML_UPDATE_STATUS_CTID_CHUNKS_WITH_ERRORS);
            updateStatus.setString(1, status.toString());
            updateStatus.setString(2, errMsg);
            updateStatus.setLong(3, this.getId());
            updateStatus.setString(4, this.getConfig().fromTaskName());
        }
        int rows = updateStatus.executeUpdate();
        updateStatus.close();
        connection.commit();
        return this;
    }

    @Override
    public ResultSet getData(Connection connection, String query) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setFetchSize(10000);
        return preparedStatement.executeQuery();
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
        return PGKeywords.SELECT + " " +
            columnToColumn + " " +
            PGKeywords.FROM + " " +
            getConfig().fromSchemaName() +
            "." +
            getConfig().fromTableName() + " " +
            (getConfig().fromTableNameAdds() == null ? "" : getConfig().fromTableNameAdds()) + " " +
            PGKeywords.WHERE + " " +
            getConfig().fetchWhereClause() +
            " and ctid >= '(" + getStart() + ",1)' and ctid < '(" + getEnd() + ",1)'";
    }
}
