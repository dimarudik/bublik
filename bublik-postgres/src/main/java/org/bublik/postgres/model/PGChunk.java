package org.bublik.postgres.model;

import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.model.Chunk;
import org.bublik.core.model.Config;
import org.bublik.core.model.LogMessage;
import org.bublik.core.model.Table;
import org.bublik.core.storage.JDBCStorage;
import org.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.bublik.postgres.constants.SQLConstants.*;

public class PGChunk<K extends Integer, T extends Long, S extends Connection, R extends ResultSet> extends Chunk<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(PGChunk.class);

    public PGChunk(K id, T start, T end, Config config, Table sourceTable,
                   ChunkStatus status, String fetchQuery, Storage sourceStorage) {
        super(id, start, end, config, sourceTable, status, fetchQuery, sourceStorage);
    }

    @Override
    public PGChunk<?, ?, ?, ?> saveChunkStatus(ChunkStatus status, boolean sync, Integer errNum,
                                      String errMsg, String chunkTableName) throws SQLException {
        if (status != null) {
            Connection connection = this.getSourceSession();
            PreparedStatement updateStatus;
            if (errMsg == null) {
                updateStatus = connection.prepareStatement(
                        DML_UPDATE_STATUS_CHUNK_TABLE.replace("$tableName", chunkTableName));
                updateStatus.setString(1, status.toString());
                updateStatus.setLong(2, this.getId());
            } else {
                updateStatus = connection.prepareStatement(
                        DML_UPDATE_STATUS_CHUNK_TABLE_WITH_ERRORS.replace("$tableName", chunkTableName));
                updateStatus.setString(1, status.toString());
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
    public Chunk<K, T, S, R> saveChunkRows(int copied, boolean sync, String chunkTableName) throws SQLException {
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
    public Chunk<?, ?, ?, R> assignSourceResultSet() throws SQLException {
        setStartTime(System.currentTimeMillis());
        String q;
        if (getConfig().columnToColumn() == null && getConfig().expressionToColumn() == null) {
            q = getSourceStorage().buildFetchStatement(getConfig(), getSourceTable());
        } else {
            q = getSourceStorage().buildFetchStatement(getConfig());
        }
        ResultSet resultSet = getData(q);
        setResultSet((R) resultSet);
        return this;
    }

    @Override
    public R getData(String query) throws SQLException {
        Connection connection = (Connection) this.getSourceSession();
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setLong(1, this.getStart());
        statement.setLong(2, this.getEnd());
        statement.setFetchSize(10000);
        return (R) statement.executeQuery();
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
    public Chunk<K, T, S, R> assignTargetSession() throws SQLException {
        if (getTargetStorage() instanceof JDBCStorage) {
            JDBCStorage targetJDBCStorage = getTargetStorage().unwrap(JDBCStorage.class);
            Connection targetConnection = targetJDBCStorage.getPoolConnection();
            setTargetSession((S) targetConnection);
        }
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
