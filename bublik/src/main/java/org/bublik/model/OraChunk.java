package org.bublik.model;

import org.bublik.constants.ChunkStatus;
import org.bublik.constants.PGKeywords;
import org.bublik.storage.JDBCStorage;
import org.bublik.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.bublik.constants.SQLConstants.*;
import static org.bublik.exception.Utils.getStackTrace;

public class OraChunk<T extends RowId> extends Chunk<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OraChunk.class);

    public OraChunk(Integer id, T start, T end, Config config, Table sourceTable, String fetchQuery, Storage sourceStorage) {
        super(id, start, end, config, sourceTable, fetchQuery, sourceStorage);
    }


    @Override
    public OraChunk<T> setChunkStatus(ChunkStatus status, Integer errNum, String errMsg) {
        try {
            Connection connection = this.getSourceConnection();
            if (errMsg == null) {
                CallableStatement callableStatement =
                        connection.prepareCall(PLSQL_UPDATE_STATUS_ROWID_CHUNKS);
                callableStatement.setString(1, this.getConfig().fromTaskName());
                callableStatement.setInt(2, this.getId());
                callableStatement.setInt(3, status.ordinal());
                callableStatement.execute();
                callableStatement.close();
            } else {
                CallableStatement callableStatement =
                        connection.prepareCall(PLSQL_UPDATE_STATUS_ROWID_CHUNKS_WITH_ERRORS);
                callableStatement.setString(1, this.getConfig().fromTaskName());
                callableStatement.setInt(2, this.getId());
                callableStatement.setInt(3, status.ordinal());
                callableStatement.setString(4, errMsg);
                callableStatement.execute();
                callableStatement.close();
            }
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
    public void insertProcessedChunkInfo(Connection connection, int rows) throws SQLException {
        PreparedStatement chunkInsert = connection.prepareStatement(DML_INSERT_BUBLIK_OUTBOX_ROWID);
        chunkInsert.setLong(1, getId());
        chunkInsert.setString(2, String.valueOf(getStart()));
        chunkInsert.setString(3, String.valueOf(getEnd()));
        chunkInsert.setLong(4, rows);
        chunkInsert.setString(5, getConfig().fromTaskName());
        chunkInsert.setString(6, getTargetTable().getSchemaName().toLowerCase());
        chunkInsert.setString(7, getTargetTable().getFinalTableName(false));
        long r = chunkInsert.executeUpdate();
        chunkInsert.close();
    }
}
