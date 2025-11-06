package org.bublik.cassandra.model;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import org.bublik.cassandra.storage.cassandraaddons.BatchEntity;
import org.bublik.cassandra.storage.cassandraaddons.CSObject;
import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.model.Chunk;
import org.bublik.core.model.Config;
import org.bublik.core.model.LogMessage;
import org.bublik.core.model.Table;
import org.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.bublik.cassandra.constants.SQLConstants.DML_DELETE_CHUNK_BY_ID;
import static org.bublik.cassandra.constants.SQLConstants.DML_INSERT_CHUNK_TABLE;

public class CSChunk<K extends UUID, T extends Long, S extends CqlSession, R extends ResultSet> extends Chunk<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(CSChunk.class);

    public CSChunk(K id, T start, T end, Config config, Table sourceTable,
                   ChunkStatus status, String fetchQuery, Storage sourceStorage, Storage targetStorage) {
        super(id, start, end, config, sourceTable, status, fetchQuery, sourceStorage, targetStorage);
    }

    @Override
    public Chunk<K, T, S, R> interStageSaveChunkStatus(ChunkStatus newStatus, boolean sync, Integer errNum,
                                                       String errMsg, String chunkTableName) throws SQLException {
        CqlSession cqlSession = getSourceSession();
        boolean applied;
        try {
            PreparedStatement psDelete = cqlSession.prepare(DML_DELETE_CHUNK_BY_ID.replace("$tableName", chunkTableName));
            BoundStatement bsDelete = psDelete.bind(getId(), getChunkStatus().toString());
            applied = cqlSession.execute(bsDelete).wasApplied();
        } catch (Exception e) {
//            log.error("Error deleting old chunk status: {} for chunk: {} with errMsg: {}", getChunkStatus(), getId(), getStackTrace(e));
            throw new SQLException(e);
        }
        setChunkStatus(newStatus);
        if (applied) {
            try {
                PreparedStatement psInsert = cqlSession.prepare(DML_INSERT_CHUNK_TABLE.replace("$tableName", chunkTableName));
                BoundStatement bsInsert = psInsert.bind(
                                getId(),
                                getStart(),
                                getEnd(),
                                getSourceTable().getSchemaName(),
                                getSourceTable().getTableName(),
                                newStatus.toString(),
                                getConfig().fromTaskName(),
                                errMsg);
                cqlSession.execute(bsInsert);
            } catch (Exception e) {
//                log.error("Error inserting new chunk status: {} for chunk: {} with errMsg: {}", newStatus, getId(), errMsg, e);
                throw new SQLException(e);
            }
        } else {
            throw new SQLException("Failed to delete chunk with id: " + getId() + " and status: " + getChunkStatus());
        }
        return this;
    }

    @Override
    public Chunk<K, T, S, R> interStageSaveChunkRows(int rows, boolean sync, String chunkTableName) {
        return this;
    }

    @Override
    public void lastStageCloseSourceSession(boolean sync) {
    }


    @Override
    public Chunk<K, T, S, R> secondStageGetSourceResultSet() throws SQLException {
        setStartTime(System.currentTimeMillis());
//        String q = getSourceStorage().buildFetchStatement(getConfig(), getSourceTable());
        String q = getSourceStorage().buildFetchStatement(getConfig(), this);
//        log.info("Fetch query: {}", q);
        ResultSet resultSet = getData(q);
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
                    .setTimeout(Duration.ofSeconds(1));
            ResultSet resultSet = cqlSession.execute(boundStatement);
            return (R) resultSet;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Chunk<K, T, S, R> mainStageTransfer(String tableName) throws SQLException {
        LogMessage logMessage = this.getTargetStorage().transfer(this, tableName);
//        LogMessage logMessage = transfer(this, tableName);
        this.setLogMessage(logMessage);
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
                .interStageSaveChunkRows(getRows(), sync, tableName)
                .interStageSaveChunkStatus(ChunkStatus.PROCESSED, sync, null, null, tableName);
//                .closeChunkSourceSession(sync);
//        LogMessage logMessage = getLogMessage();
        logChunkInfo();
        return this;
    }
}
