package org.bublik.postgres.model;

import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.model.Chunk;
import org.bublik.core.model.Config;
import org.bublik.core.model.Table;
import org.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.bublik.postgres.constants.SQLConstants.*;

public class PGChunk<T extends Long> extends Chunk<T> {
    private static final Logger log = LoggerFactory.getLogger(PGChunk.class);

    public PGChunk(Integer id, T start, T end, Config config,
                   Table sourceTable, String fetchQuery, Storage sourceStorage) {
        super(id, start, end, config, sourceTable, fetchQuery, sourceStorage);
    }

    @Override
    public PGChunk<T> saveChunkStatus(ChunkStatus status, boolean sync, Integer errNum,
                                      String errMsg, String chunkTableName) throws SQLException {
        if (status != null) {
            Connection connection = this.getSourceConnection();
            PreparedStatement updateStatus;
            if (errMsg == null) {
                updateStatus = connection.prepareStatement(
                        DML_UPDATE_STATUS_CHUNK_TABLE.replace("$tableName", chunkTableName));
//                updateStatus.setString(1, getUuid().toString());
                updateStatus.setString(1, status.toString());
                updateStatus.setLong(2, this.getId());
                updateStatus.setString(3, this.getConfig().fromTaskName());
            } else {
                updateStatus = connection.prepareStatement(
                        DML_UPDATE_STATUS_CHUNK_TABLE_WITH_ERRORS.replace("$tableName", chunkTableName));
                updateStatus.setString(1, status.toString());
                updateStatus.setString(2, errMsg.substring(0,
                        errMsg.length() > 2048 ? 2047 : errMsg.length()));
                updateStatus.setLong(3, this.getId());
                updateStatus.setString(4, this.getConfig().fromTaskName());
            }
            int rows = updateStatus.executeUpdate();
            updateStatus.close();
            if (!sync)
                connection.commit();
        }
//        LOGGER.debug("setChunkStatus {}", status);
        return this;
    }

    @Override
    public Chunk<?> saveChunkRows(int copied, boolean sync, String chunkTableName) throws SQLException {
        Connection connection = this.getSourceConnection();
        PreparedStatement updateStatus;
        updateStatus = connection.prepareStatement(DML_UPDATE_UUID_COPIED_CHUNK_TABLE.replace("$tableName", chunkTableName));
//        updateStatus.setString(1, getUuid().toString());
        updateStatus.setInt(1, copied);
        updateStatus.setInt(2, this.getId());
        int n = updateStatus.executeUpdate();
        updateStatus.close();
        if (!sync)
            connection.commit();
        return this;
    }

/*
    public Chunk<?> saveConfig(boolean sync) throws SQLException {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String jacksonData = objectMapper.writeValueAsString(getConfig());
            Connection connection = this.getSourceConnection();
            PreparedStatement updateStatus;
            updateStatus = connection.prepareStatement(DML_UPDATE_CONFIG_CTID_CHUNKS);
            updateStatus.setString(1, jacksonData);
            updateStatus.setInt(2, this.getId());
            int n = updateStatus.executeUpdate();
            updateStatus.close();
            if (!sync)
                connection.commit();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return this;
    }
*/

    @Override
    public ResultSet getData(Connection connection, String query) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setLong(1, this.getStart());
        statement.setLong(2, this.getEnd());
        statement.setFetchSize(10000);
        setPreparedStatement(statement);
        return statement.executeQuery();
    }
}
