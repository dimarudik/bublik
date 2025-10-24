package org.bublik.oracle.model;

import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.model.Chunk;
import org.bublik.core.model.Config;
import org.bublik.core.model.LogMessage;
import org.bublik.core.model.Table;
import org.bublik.core.storage.JDBCStorage;
import org.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

import static org.bublik.oracle.constants.SQLConstants.*;

public class OraChunk<K extends Integer, T extends RowId, S extends Connection, R extends ResultSet> extends Chunk<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(OraChunk.class);

    public OraChunk(K id, T start, T end, Config config, Table sourceTable,
                    ChunkStatus status, String fetchQuery, Storage sourceStorage) {
        super(id, start, end, config, sourceTable, status, fetchQuery, sourceStorage);
    }

    @Override
    public OraChunk<K, T, S, R> saveChunkStatus(ChunkStatus status, boolean sync, Integer errNum, String errMsg, String chunkTableName) {
        try {
            Connection connection = this.getSourceSession();
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
    public Chunk<K, T, S, R> assignSourceResultSet() throws SQLException {
        setStartTime(System.currentTimeMillis());
        String q;
        if (getConfig().columnToColumn() == null && getConfig().expressionToColumn() == null) {
            q = getSourceStorage().buildFetchStatement(getConfig(), getSourceTable());
        } else {
            q = getSourceStorage().buildFetchStatement(getConfig());
        }
        ResultSet resultSet = (ResultSet) getData(q);
        setResultSet((R) resultSet);
        return this;
    }

    @Override
    public Chunk<K, T, S, R> saveChunkRows(int rows, boolean sync, String chunkTableName) throws SQLException {
        return this;
    }

    @Override
    public R getData(String query) throws SQLException {
        Connection connection = this.getSourceSession();
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setRowId(1, this.getStart());
        statement.setRowId(2, this.getEnd());
        statement.setFetchSize(10000);
        return (R) statement.executeQuery();
    }

    @Override
    public Chunk<K, T, S, R> assignTargetSession() throws SQLException {
        if (getTargetStorage() instanceof JDBCStorage) {
            JDBCStorage targetJDBCStorage = getTargetStorage().unwrap(JDBCStorage.class);
            Connection targetConnection = targetJDBCStorage.getPoolConnection();
            setTargetSession((S) targetConnection);
        }
        return this;
    }

    @Override
    public Chunk<?, ?, ?, ?> saveChunkStatus(ChunkStatus status, boolean sync, String chunkTableName) throws SQLException {
        return super.saveChunkStatus(status, sync, chunkTableName);
    }

    @Override
    public Chunk<K, T, S, R> copyChunk(boolean sync, String tableName) throws SQLException {
        this
                .assignSourceSession()
                .assignTargetSession()
                .saveChunkStatus(ChunkStatus.ASSIGNED, sync, null, null, tableName)
                .assignSourceResultSet()
                .assignResultLogMessage(tableName)
                .saveChunkRows(getRows(), sync, tableName)
                .saveChunkStatus(ChunkStatus.PROCESSED, sync, null, null, tableName)
                .closeChunkSourceSession(sync);
        LogMessage logMessage = getLogMessage();
        logMessage.loggerChunkInfo();
        if (getSourceSession().isValid(0)) {
            getSourceSession().close();
        }
        return this;
    }

    @Override
    public void closeChunkSourceSession(boolean sync) throws SQLException{
        getSourceSession().close();
    }

    @Override
    public Chunk<K, T, S, R> assignSourceSession() throws SQLException {
        JDBCStorage sourceJDBCStorage = getSourceStorage().unwrap(JDBCStorage.class);
        Connection sourceConnection = sourceJDBCStorage.getPoolConnection();
        setSourceSession((S)sourceConnection);
        return this;
    }

    @Override
    public Chunk<K, T, S, R> assignResultLogMessage(String tableName) throws SQLException {
        try {
            LogMessage logMessage = this.getTargetStorage().transferToTarget(this, tableName);
            this.setLogMessage(logMessage);
            getResultSet().close();
            return this;
        } catch (SQLException | RuntimeException e) {
            this.setLogMessage(new LogMessage (0, 0, 0, " UNREACHABLE TASK ", this));
            throw e;
        }
    }
}
