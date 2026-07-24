package dev.bublik.oracle.model;

import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.model.*;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

import static dev.bublik.oracle.constants.SQLConstants.PLSQL_UPDATE_STATUS_ROWID_CHUNKS;
import static dev.bublik.oracle.constants.SQLConstants.PLSQL_UPDATE_STATUS_ROWID_CHUNKS_WITH_ERRORS;

public class OraChunk<K extends Integer, T extends RowId, S extends Connection, R extends ResultSet> extends Chunk<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(OraChunk.class);

    public OraChunk(K id, T start, T end, Config config, Table2Table t2t,
                    ChunkStatus status, String fetchQuery, Storage sourceStorage,
                    Storage targetStorage, String orderByClause) {
        super(id, start, end, config, t2t, status, fetchQuery, sourceStorage, targetStorage, orderByClause);
    }

    @Override
    public boolean isValidSourceSession() throws SQLException {
        return getSourceSession() != null && getSourceSession().isValid(1);
    }

    @Override
    public boolean isValidTargetSession() throws SQLException {
        return getTargetSession() != null && getTargetSession().isValid(1);
    }

    @Override
    public OraChunk<K, T, S, R> interStageSaveChunkStatus(ChunkStatus newStatus, boolean sync, Integer errNum, String errMsg, String chunkTableName) {
        try {
            Connection connection = this.getSourceSession();
            if (errMsg == null) {
                CallableStatement callableStatement =
                        connection.prepareCall(PLSQL_UPDATE_STATUS_ROWID_CHUNKS);
                callableStatement.setString(1, this.getConfig().fromTaskName());
                callableStatement.setInt(2, this.getId());
                callableStatement.setInt(3, newStatus.ordinal());
                callableStatement.execute();
                callableStatement.close();
            } else {
                CallableStatement callableStatement =
                        connection.prepareCall(PLSQL_UPDATE_STATUS_ROWID_CHUNKS_WITH_ERRORS);
                callableStatement.setString(1, this.getConfig().fromTaskName());
                callableStatement.setInt(2, this.getId());
                callableStatement.setInt(3, newStatus.ordinal());
//                callableStatement.setString(4, errMsg.substring(0, 2244));
                callableStatement.setString(4, errMsg.substring(0,
                        errMsg.length() > 2245 ? 2244 : errMsg.length()));
                callableStatement.execute();
                callableStatement.close();
            }
        } catch (SQLException e) {
            throw  new RuntimeException(e);
        }
        return this;
    }

    @Override
    public Chunk<K, T, S, R> secondStageGetSourceResultSet() throws SQLException {
        setStartTime(System.currentTimeMillis());
        String q = getFetchQuery();
        ResultSet resultSet = getData(q);
        setResultSet((R) resultSet);
        return this;
    }

    @Override
    public Chunk<K, T, S, R> interStageSaveChunkRows(int copied, boolean sync, String chunkTableName) throws SQLException {
        return this;
    }

    @Override
    public R getData(String query) throws SQLException {
        Connection connection = this.getSourceSession();
        String sql = query + getOrderByClause();
//        log.info("sql: {}", sql);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setRowId(1, this.getStart());
        statement.setRowId(2, this.getEnd());
        statement.setFetchSize(getSourceStorage().getFetchSize());
        return (R) statement.executeQuery();
    }

    @Override
    public Chunk<K, T, S, R> allStages(boolean sync, Table tableName) throws SQLException {
        this
                .firstStageAssignSourceSession(this)
                .firstStageAssignTargetSession(this)
                .interStageSaveChunkStatus(ChunkStatus.ASSIGNED, sync, null, null, tableName.tableToString())
                .secondStageGetSourceResultSet()
                .mainStageTransfer(tableName.tableToString())
                .interStageSaveChunkRows(getCopied(), sync, tableName.tableToString())
                .interStageSaveChunkStatus(ChunkStatus.PROCESSED, sync, null, null, tableName.tableToString())
                .lastStageCloseSourceSession(sync);
        logChunkInfo();
        if (getSourceSession().isValid(0)) {
            getSourceSession().close();
        }
        if (getTargetStorage() instanceof JDBCStorage && getTargetSession().isValid(0)) {
            getTargetSession().close();
        }
        return this;
    }

    @Override
    public void lastStageCloseSourceSession(boolean sync) throws SQLException{
        getSourceSession().close();
    }

    @Override
    public Chunk<K, T, S, R> mainStageTransfer(String tableName) throws SQLException {
        try {
            LogMessage logMessage = this.getTargetStorage().transfer(this, tableName);
            this.setLogMessage(logMessage);
            getResultSet().close();
            return this;
        } catch (SQLException | RuntimeException e) {
            this.setLogMessage(new LogMessage (0, 0, " UNREACHABLE TASK "));
            throw e;
        }
    }
}
