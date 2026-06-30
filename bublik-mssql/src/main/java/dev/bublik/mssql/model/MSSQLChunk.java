package dev.bublik.mssql.model;

import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.model.*;
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
    private String addFetchPredicate;

    public MSSQLChunk(K id, T start, T end, Config config, Table2Table t2t,
                      ChunkStatus status, String fetchQuery, Storage sourceStorage,
                      Storage targetStorage, String orderByClause) {
        super(id, start, end, config, t2t, status, fetchQuery, sourceStorage, targetStorage, orderByClause);
    }

    @Override
    public R getData(String query) throws SQLException {
        List<Object> start = this.getStart();
        List<Object> end = this.getEnd();
        String sql = query;
        if (end.getFirst() != null) {
            sql = sql + addFetchPredicate;
        }
        sql = sql + (getOrderByClause() == null ? "" : getOrderByClause());
//        log.info("{} {} {}", sql, start, end);
        Connection connection = this.getSourceSession();
        PreparedStatement statement = connection.prepareStatement(sql);
        int paramIndex = 1;
        for (int i = 0; i < start.size(); i++) {
            for (int j = 0; j <= i; j++) {
                statement.setObject(paramIndex++, start.get(j));
            }
        }
        if (end.getFirst() != null) {
            for (int i = 0; i < end.size(); i++) {
                for (int j = 0; j <= i; j++) {
                    statement.setObject(paramIndex++, end.get(j));
                }
            }
        }
        statement.setFetchSize(10_000);
        return (R) statement.executeQuery();
    }

    private String schemaName() {
        return getSourceStorage().getOutboxTable().getSchemaName() == null ? "bublik"
                : getSourceStorage().getOutboxTable().getSchemaName();
    }

    @Override
    public Chunk<K, T, S, R> allStages(boolean sync, Table tableName) throws SQLException {
        String oTable = schemaName() + ".[" + tableName.getTableName() + "]";
        this
                .firstStageAssignSourceSession(this)
                .firstStageAssignTargetSession(this)
                .interStageSaveChunkStatus(ChunkStatus.ASSIGNED, sync, null, null, oTable)
                .secondStageGetSourceResultSet()
                .mainStageTransfer(oTable)
                .interStageSaveChunkRows(getCopied(), sync, oTable)
                .interStageSaveChunkStatus(ChunkStatus.PROCESSED, sync, null, null, oTable)
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
//                System.out.println(DML_UPDATE_STATUS_CHUNK_TABLE.replace("$tableName", chunkTableName));
                updateStatus = connection.prepareStatement(
                        DML_UPDATE_STATUS_CHUNK_TABLE
                                .replace("$tableName", chunkTableName));
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

    public String getAddFetchPredicate() {
        return addFetchPredicate;
    }

    public void setAddFetchPredicate(String addFetchPredicate) {
        this.addFetchPredicate = addFetchPredicate;
    }
}
