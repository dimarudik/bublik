package dev.bublik.mssql.model;

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
import java.util.List;

import static dev.bublik.mssql.constants.SQLConstants.*;

public class MSSQLChunk<K extends Integer, T extends List<Object>, S extends Connection, R extends ResultSet> extends Chunk<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(MSSQLChunk.class);
    private String addFetchQuery;

    public MSSQLChunk(K id, T start, T end, Config config, Table2Table<S> t2t,
                      ChunkStatus status, String fetchQuery, Storage<K, T, S, R> sourceStorage, Storage<K, T, S, R> targetStorage) {
        super(id, start, end, config, t2t, status, fetchQuery, sourceStorage, targetStorage);
    }

    @Override
    public R getData(String query) throws SQLException {
        List<Object> start = this.getStart();
        List<Object> end = this.getEnd();
        String sql = query;
        if (end.getFirst() != null) {
            sql = sql + addFetchQuery;
        }
        Connection connection = this.getSourceSession();
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i < start.size(); i++) {
            statement.setObject(i + 1, start.get(i));
            if (end.get(i) != null) {
                statement.setObject(i + 1 + start.size(), end.get(i));
            }
        }
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
    public MSSQLChunk<K, T, S, R> interStageSaveChunkStatus(ChunkStatus newStatus, boolean sync, Integer errNum,
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
    public Chunk<K, T, S, R> secondStageGetSourceResultSet() throws SQLException {
        setStartTime(System.currentTimeMillis());
        String q = getFetchQuery();
        ResultSet resultSet = getData(q);
        setResultSet((R) resultSet);
        return this;
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
    public void lastStageCloseSourceSession(boolean sync) throws SQLException{
        getSourceSession().close();
    }

    public String getAddFetchQuery() {
        return addFetchQuery;
    }

    public void setAddFetchQuery(String addFetchQuery) {
        this.addFetchQuery = addFetchQuery;
    }
}
