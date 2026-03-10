package dev.bublik.mssql.model;

import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.model.Chunk;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.Table2Table;
import dev.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MSSQLChunk<K extends Integer, T extends String, S extends Connection, R extends ResultSet> extends Chunk<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(MSSQLChunk.class);

    public MSSQLChunk(K id, T start, T end, Config config, Table2Table<S> t2t,
                      ChunkStatus status, String fetchQuery, Storage<K, T, S, R> sourceStorage, Storage<K, T, S, R> targetStorage) {
        super(id, start, end, config, t2t, status, fetchQuery, sourceStorage, targetStorage);
    }

    @Override
    public R getData(String query) throws SQLException {
        return null;
    }

    @Override
    public Chunk<K, T, S, R> allStages(boolean sync, String tableName) throws SQLException {
        return null;
    }

    @Override
    public Chunk<K, T, S, R> interStageSaveChunkStatus(ChunkStatus newStatus, boolean sync, Integer errNum, String errMsg, String chunkTableName) throws SQLException {
        return null;
    }

    @Override
    public Chunk<K, T, S, R> secondStageGetSourceResultSet() throws SQLException {
        return null;
    }

    @Override
    public Chunk<K, T, S, R> mainStageTransfer(String tableName) throws SQLException {
        return null;
    }

    @Override
    public Chunk<K, T, S, R> interStageSaveChunkRows(int rows, boolean sync, String chunkTableName) throws SQLException {
        return null;
    }

    @Override
    public void lastStageCloseSourceSession(boolean sync) throws SQLException {

    }
}
