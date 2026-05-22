package dev.bublik.cassandra.model;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
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
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static dev.bublik.cassandra.constants.SQLConstants.*;
import static dev.bublik.core.util.Utils.getStackTrace;

public class CSChunk<K extends UUID, T extends Long, S extends CqlSession, R extends ResultSet> extends Chunk<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(CSChunk.class);

    public CSChunk(K id, T start, T end, Config config, Table2Table<S> t2t,
                   ChunkStatus status, String fetchQuery, Storage sourceStorage,
                   Storage targetStorage, String orderByClause) {
        super(id, start, end, config, t2t, status, fetchQuery, sourceStorage, targetStorage, orderByClause);
    }

    @Override
    public Chunk<K, T, S, R> interStageSaveChunkStatus(ChunkStatus newStatus, boolean sync, Integer errNum,
                                                       String errMsg, String chunkTableName) throws SQLException {
        CqlSession cqlSession = getSourceSession();
        boolean applied;
        try {
            PreparedStatement psDelete = cqlSession.prepare(DML_DELETE_CHUNK_BY_ID.replace("$tableName", chunkTableName));
            BoundStatement bsDelete = psDelete.bind(getId(), getChunkStatus().toString(), getConfig().fromSchemaName(), getConfig().fromTableName())
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            applied = cqlSession.execute(bsDelete).wasApplied();
        } catch (Exception e) {
            throw new SQLException(e);
        }
        setChunkStatus(newStatus);
        if (applied) {
            try {
                PreparedStatement psInsert = cqlSession.prepare(DML_INSERT_CHUNK_TABLE.replace("$tableName", chunkTableName));
                BoundStatement bsInsert = psInsert.boundStatementBuilder()
                        .setUuid("chunk_id", getId())
                        .setLong("start_page", getStart())
                        .setLong("end_page", getEnd())
                        .setString("schema_name", getT2t().sourceTable().getSchemaName())
                        .setString("table_name", getT2t().sourceTable().getTableName())
                        .setString("status", newStatus.toString())
                        .setString("task_name", getConfig().fromTaskName())
                        .setString("err_msg", errMsg)
                        .setInt("copied", getCopied())
                        .setString("thread", Thread.currentThread().getName())
                        .setInstant("start_ts", getStartTs())
                        .setInstant("end_ts", getEndTs())
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                        .build();
                cqlSession.execute(bsInsert);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        } else {
            throw new SQLException("Failed to delete chunk with id: " + getId() + " and status: " + getChunkStatus());
        }
        return this;
    }

    @Override
    public Chunk<K, T, S, R> interStageSaveChunkRows(int copied, boolean sync, String chunkTableName) {
        CqlSession cqlSession = getSourceSession();
        try {
            PreparedStatement ps = cqlSession.prepare(DML_UPDATE_ROWS_CHUNK_TABLE.replace("$tableName", chunkTableName));
            BoundStatement bsUpdate = ps.boundStatementBuilder()
                    .setInt("copied", copied)
                    .setInstant("end_ts", LocalDateTime.now().toInstant(ZoneOffset.of("+0")))
//                    .setInstant("end_ts", Instant.now())
                    .setUuid("chunk_id", getId())
                    .setString("status", getChunkStatus().toString())
                    .setString("schema_name", getT2t().sourceTable().getSchemaName())
                    .setString("table_name", getT2t().sourceTable().getTableName())
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                    .build();
            cqlSession.execute(bsUpdate);
        } catch (Exception e) {
            log.error("{}", getStackTrace(e));
            throw new RuntimeException(e);
        }
//        log.info("Chunk with id: {} and status: {} updated with rows: {}", getId(), getChunkStatus(), rows);
        return this;
    }

    @Override
    public void lastStageCloseSourceSession(boolean sync) {
    }

    @Override
    public Chunk<K, T, S, R> secondStageGetSourceResultSet() throws SQLException {
        setStartTime(System.currentTimeMillis());
//        String sql = getFetchQuery() + (getOrderByClause() == null ? "" : getOrderByClause());
        String sql = getFetchQuery();
//        log.info("{} {} {}", sql, getStart(), getEnd());
        ResultSet resultSet = getData(sql);
        setResultSet((R) resultSet);
        return this;
    }

    @Override
    public R getData(String query) throws SQLException{
        try {
            CqlSession cqlSession = this.getSourceSession();
            PreparedStatement statement = cqlSession.prepare(query);
            BoundStatement boundStatement = statement.bind(getStart(), getEnd())
                    .setPageSize(1_000)
                    .setTimeout(Duration.ofSeconds(1))
                    .setConsistencyLevel(ConsistencyLevel.QUORUM);
            ResultSet resultSet = cqlSession.execute(boundStatement);
            return (R) resultSet;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Chunk<K, T, S, R> mainStageTransfer(String tableName) throws SQLException {
        LogMessage logMessage = this.getTargetStorage().transfer(this, tableName);
        setEndTs(LocalDateTime.now().toInstant(ZoneOffset.of("+0")));
        setLogMessage(logMessage);
        return this;
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
                .interStageSaveChunkStatus(ChunkStatus.PROCESSED, sync, null, null, tableName);
        logChunkInfo();
        if (getTargetStorage() instanceof JDBCStorage && ((Connection)getTargetSession()).isValid(0)) {
            ((Connection)getTargetSession()).close();
        }
        return this;
    }
}
