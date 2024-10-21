package org.bublik.model;

import org.bublik.constants.ChunkStatus;
import org.bublik.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.bublik.constants.SQLConstants.DML_UPDATE_STATUS_CTID_CHUNKS_WITH_ERRORS;
import static org.bublik.constants.SQLConstants.PLSQL_UPDATE_STATUS_CTID_CHUNKS;

public class PGChunk<T extends Long> extends Chunk<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PGChunk.class);
    public PGChunk(Integer id, T start, T end, Config config, Table sourceTable, String fetchQuery, Storage sourceStorage) {
        super(id, start, end, config, sourceTable, fetchQuery, sourceStorage);
    }

    @Override
    public PGChunk<T> setChunkStatus(ChunkStatus status, Integer errNum, String errMsg) throws SQLException {
        Connection connection = this.getSourceConnection();
        PreparedStatement updateStatus;
        if (errMsg == null) {
            updateStatus = connection.prepareStatement(PLSQL_UPDATE_STATUS_CTID_CHUNKS);
            updateStatus.setString(1, status.toString());
            updateStatus.setLong(2, this.getId());
            updateStatus.setString(3, this.getConfig().fromTaskName());
        } else {
            updateStatus = connection.prepareStatement(DML_UPDATE_STATUS_CTID_CHUNKS_WITH_ERRORS);
            updateStatus.setString(1, status.toString());
            updateStatus.setString(2, errMsg);
            updateStatus.setLong(3, this.getId());
            updateStatus.setString(4, this.getConfig().fromTaskName());
        }
        int rows = updateStatus.executeUpdate();
        updateStatus.close();
        connection.commit();
        return this;
    }

    @Override
    public ResultSet getData(Connection connection, String query) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setLong(1, this.getStart());
        statement.setLong(2, this.getEnd());
        statement.setFetchSize(10000);
        return statement.executeQuery();
    }
}
