package org.bublik.cassandra.model;

import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.model.Chunk;
import org.bublik.core.model.Config;
import org.bublik.core.model.Table;
import org.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class CSChunk<T extends Integer, K extends UUID> extends Chunk<T, K> {
    private static final Logger log = LoggerFactory.getLogger(CSChunk.class);

    public CSChunk(K id, T start, T end, Config config, Table sourceTable, String fetchQuery, Storage sourceStorage) {
        super(id, start, end, config, sourceTable, fetchQuery, sourceStorage);
    }

    @Override
    public Chunk<?, ?> saveChunkStatus(ChunkStatus status, boolean sync, Integer errNum, String errMsg, String chunkTableName) throws SQLException {
        return null;
    }

    @Override
    public Chunk<?, ?> saveChunkRows(int rows, boolean sync, String chunkTableName) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getData(Connection connection, String query) throws SQLException {
        return null;
    }
}
