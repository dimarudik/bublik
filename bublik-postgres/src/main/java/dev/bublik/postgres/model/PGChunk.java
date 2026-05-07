package dev.bublik.postgres.model;

import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.model.Chunk;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.LogMessage;
import dev.bublik.core.model.Table2Table;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static dev.bublik.postgres.constants.SQLConstants.*;

public class PGChunk<K extends Integer, T extends Long, S extends Connection, R extends ResultSet> extends Chunk<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(PGChunk.class);

    public PGChunk(K id, T start, T end, Config config, Table2Table<S> t2t,
                   ChunkStatus status, String fetchQuery, Storage sourceStorage,
                   Storage targetStorage, String orderByClause) {
        super(id, start, end, config, t2t, status, fetchQuery, sourceStorage, targetStorage, orderByClause);
    }

    @Override
    public PGChunk<K, T, S, R> interStageSaveChunkStatus(ChunkStatus newStatus, boolean sync, Integer errNum,
                                                         String errMsg, String chunkTableName) throws SQLException {
        if (newStatus != null) {
            Connection connection = this.getSourceSession();
            PreparedStatement updateStatus;
            if (errMsg == null) {
                updateStatus = connection.prepareStatement(
                        DML_UPDATE_STATUS_CHUNK_TABLE.replace("$tableName", chunkTableName));
                updateStatus.setString(1, newStatus.toString());
                updateStatus.setLong(2, this.getId());
            } else {
                updateStatus = connection.prepareStatement(
                        DML_UPDATE_STATUS_CHUNK_TABLE_WITH_ERRORS.replace("$tableName", chunkTableName));
                updateStatus.setString(1, newStatus.toString());
                updateStatus.setString(2, errMsg.substring(0,
                        errMsg.length() > 2048 ? 2047 : errMsg.length()));
                updateStatus.setLong(3, this.getId());
            }
            int rows = updateStatus.executeUpdate();
            updateStatus.close();
            if (!sync)
                connection.commit();
        }
        return this;
    }

    @Override
    public Chunk<K, T, S, R> interStageSaveChunkRows(int copied, boolean sync, String chunkTableName) throws SQLException {
        Connection connection = this.getSourceSession();
        PreparedStatement updateStatus;
        updateStatus = connection.prepareStatement(DML_UPDATE_UUID_COPIED_CHUNK_TABLE.replace("$tableName", chunkTableName));
        updateStatus.setInt(1, copied);
        updateStatus.setInt(2, this.getId());
        int n = updateStatus.executeUpdate();
        updateStatus.close();
        if (!sync)
            connection.commit();
        return this;
    }

    @Override
    public Chunk<K, T, S, R> secondStageGetSourceResultSet() throws SQLException {
        setStartTime(System.currentTimeMillis());
        String sql = getFetchQuery() + (getOrderByClause() == null ? "" : getOrderByClause());
//        log.info("{} {} {}", sql, getStart(), getEnd());
/*
        String q;
        if (getConfig().columnToColumn() == null && getConfig().expressionToColumn() == null) {
            q = getSourceStorage().buildFetchStatement(getConfig(), this);
        } else {
            q = getSourceStorage().buildFetchStatement(getConfig());
        }
*/
        ResultSet resultSet = getData(sql);
        setResultSet((R) resultSet);
        return this;
    }

    @Override
    public R getData(String query) throws SQLException {
        Connection connection = this.getSourceSession();
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setLong(1, this.getStart());
        statement.setLong(2, this.getEnd());
        statement.setFetchSize(10_000);
        return (R) statement.executeQuery();
    }

    @Override
    public Chunk<K, T, S, R> allStages(boolean sync, String tableName) throws SQLException {
        this
                .firstStageAssignSourceSession(this)
                .firstStageAssignTargetSession(this)
                .interStageSaveChunkStatus(ChunkStatus.ASSIGNED, sync, null, null, tableName)
                .secondStageGetSourceResultSet()
                .mainStageTransfer(tableName)
                .interStageSaveChunkRows(getCopied(), sync, tableName)
                .interStageSaveChunkStatus(ChunkStatus.PROCESSED, sync, null, null, tableName)
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
