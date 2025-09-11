package org.bublik.oracle.model;

import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.model.Chunk;
import org.bublik.core.model.Config;
import org.bublik.core.model.Table;
import org.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

import static org.bublik.core.constants.SQLConstants.*;

public class OraChunk<T extends RowId> extends Chunk<T> {
    private static final Logger log = LoggerFactory.getLogger(OraChunk.class);

    public OraChunk(Integer id, T start, T end, Config config, Table sourceTable, String fetchQuery, Storage sourceStorage) {
        super(id, start, end, config, sourceTable, fetchQuery, sourceStorage);
    }

    @Override
    public Integer getParentId() {
        return 0;
    }

    @Override
    public Long getXidMin() {
        return 0L;
    }


    @Override
    public OraChunk<T> saveChunkStatus(ChunkStatus status, boolean sync, Integer errNum, String errMsg) {
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
    public Chunk<?> saveChunkRows(int rows, boolean sync) throws SQLException {
        return this;
    }

    @Override
    public Chunk<?> saveConfig(boolean sync) throws SQLException {
        return this;
    }

    @Override
    public ResultSet getData(Connection connection, String query) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setRowId(1, this.getStart());
        statement.setRowId(2, this.getEnd());
        statement.setFetchSize(10000);
        setPreparedStatement(statement);
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

    @Override
    public Chunk<?> saveChunkStatus(ChunkStatus status, boolean sync) throws SQLException {
        return super.saveChunkStatus(status, sync);
    }
}
