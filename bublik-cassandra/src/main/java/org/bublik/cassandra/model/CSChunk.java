package org.bublik.cassandra.model;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.model.Chunk;
import org.bublik.core.model.Config;
import org.bublik.core.model.Table;
import org.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.UUID;

import static org.bublik.cassandra.constants.SQLConstants.*;

public class CSChunk<K extends UUID, T extends Long, S extends CqlSession, R extends ResultSet> extends Chunk<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(CSChunk.class);

    public CSChunk(K id, T start, T end, Config config, Table sourceTable,
                   ChunkStatus status, String fetchQuery, Storage sourceStorage) {
        super(id, start, end, config, sourceTable, status, fetchQuery, sourceStorage);
    }

    @Override
    public Chunk<?, ?, ?, ?> saveChunkStatus(ChunkStatus status, boolean sync, Integer errNum,
                                       String errMsg, String chunkTableName) throws SQLException {
        if (status != null) {
            if (errMsg == null) {
                CqlSession cqlSession = getSourceSession();
                PreparedStatement psDelete = cqlSession.prepare(DML_DELETE_CHUNK_BY_ID.replace("$tableName", chunkTableName));
                BoundStatement bsDelete = psDelete.bind(getId(), ChunkStatus.UNASSIGNED.toString());
                if (cqlSession.execute(bsDelete).wasApplied()) {
                    PreparedStatement psInsert = cqlSession.prepare(DML_INSERT_CHUNK_TABLE.replace("$tableName", chunkTableName));
                    BoundStatement bsInsert = psInsert.bind(
                            getStart(),
                            getEnd(),
                            getSourceTable().getSchemaName(),
                            getSourceTable().getTableName(),
                            status.toString(),
                            getConfig().fromTaskName());
                    cqlSession.execute(bsInsert);
                }
            } else {
                CqlSession cqlSession = getSourceSession();
                PreparedStatement psDelete = cqlSession.prepare(DML_DELETE_CHUNK_BY_ID.replace("$tableName", chunkTableName));
                BoundStatement bsDelete = psDelete.bind(getId(), ChunkStatus.UNASSIGNED.toString());
                if (cqlSession.execute(bsDelete).wasApplied()) {
                    PreparedStatement psInsert = cqlSession.prepare(DML_INSERT_CHUNK_TABLE_WITH_ERR.replace("$tableName", chunkTableName));
                    BoundStatement bsInsert = psInsert.bind(
                            getStart(),
                            getEnd(),
                            getSourceTable().getSchemaName(),
                            getSourceTable().getTableName(),
                            status.toString(),
                            getConfig().fromTaskName(),
                            errMsg);
                    cqlSession.execute(bsInsert);
                }
            }
        }
        return this;
    }

    @Override
    public Chunk<?, ?, ?, ?> saveChunkRows(int rows, boolean sync, String chunkTableName) throws SQLException {
        return null;
    }

    @Override
    public void closeChunkSourceSession(boolean sync) throws SQLException {

    }

    @Override
    public Chunk<K, T, S, R> assignSourceResultSet() throws SQLException {
        return null;
    }

    @Override
    public R getData(String query) throws SQLException {
        return null;
    }

    @Override
    public Chunk<K, T, S, R> assignResultLogMessage(String tableName) throws SQLException {
        return null;
    }

    @Override
    public Chunk<K, T, S, R> assignSourceSession() throws SQLException {
        return null;
    }

    @Override
    public Chunk<K, T, S, R> assignTargetSession() throws SQLException {
        return null;
    }

    @Override
    public Chunk<K, T, S, R> copyChunk(boolean sync, String tableName) throws SQLException {
        return null;
    }

    @Override
    public Chunk<?, ?, ?, ?> saveChunkStatus(ChunkStatus status, boolean sync, String chunkTableName) throws SQLException {
        return super.saveChunkStatus(status, sync, chunkTableName);
    }
}
