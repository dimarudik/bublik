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

import static org.bublik.core.constants.SQLConstants.*;

public class PGChunk<T extends Long> extends Chunk<T> {
    private static final Logger log = LoggerFactory.getLogger(PGChunk.class);
    private final Integer parentId;
    private final Long xidMin;
    private final Long xidMax;

    public PGChunk(Integer id, T start, T end, Config config, Table sourceTable, String fetchQuery, Storage sourceStorage) {
        super(id, start, end, config, sourceTable, fetchQuery, sourceStorage);
        this.parentId = null;
        this.xidMin = null;
        this.xidMax = null;
    }

    public PGChunk(Integer id, T start, T end, Config config, Table sourceTable, Table targetTable, Storage sourceStorage,
                   Integer parentId, Long xidMin, Long xidMax, Connection sourceConnection, String fetchQuery, ChunkStatus chunkStatus) {
        super(id, start, end, config, sourceTable, fetchQuery, sourceStorage);
        this.parentId = parentId;
        this.xidMin = xidMin;
        this.xidMax = xidMax;
        this.setTargetTable(targetTable);
        this.setSourceConnection(sourceConnection);
        this.setChunkStatus(chunkStatus);
    }

    public Integer getParentId() {
        return parentId;
    }

    public Long getXidMin() {
        return xidMin;
    }

    public Long getXidMax() {
        return xidMax;
    }

    @Override
    public PGChunk<T> saveChunkStatus(ChunkStatus status, boolean sync, Integer errNum, String errMsg) throws SQLException {
        if (status != null) {
            Connection connection = this.getSourceConnection();
            PreparedStatement updateStatus;
            if (errMsg == null) {
                updateStatus = connection.prepareStatement(DML_UPDATE_STATUS_CTID_CHUNKS);
                updateStatus.setString(1, status.toString());
                updateStatus.setLong(2, this.getId());
                updateStatus.setString(3, this.getConfig().fromTaskName());
            } else {
                updateStatus = connection.prepareStatement(DML_UPDATE_STATUS_CTID_CHUNKS_WITH_ERRORS);
                updateStatus.setString(1, status.toString());
                updateStatus.setString(2, errMsg.substring(0, errMsg.length() > 2048 ? 2047 : errMsg.length()));
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
    public Chunk<?> saveChunkRows(int copied, boolean sync) throws SQLException {
        Connection connection = this.getSourceConnection();
        PreparedStatement updateStatus;
        updateStatus = connection.prepareStatement(DML_UPDATE_COPIED_CTID_CHUNKS);
        updateStatus.setInt(1, copied);
        updateStatus.setInt(2, this.getId());
        int n = updateStatus.executeUpdate();
        updateStatus.close();
        if (!sync)
            connection.commit();
        return this;
    }

    @Override
    public Chunk<?> saveConfig(boolean sync) throws SQLException {
/*
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
*/
        return this;
    }

    @Override
    public ResultSet getData(Connection connection, String query) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setLong(1, this.getStart());
        statement.setLong(2, this.getEnd());
        statement.setFetchSize(10000);
        setPreparedStatement(statement);
        return statement.executeQuery();
    }

    @Override
    public void insertProcessedChunkInfo(Connection connection, int rows) throws SQLException {
        PreparedStatement chunkInsert = connection.prepareStatement(DML_INSERT_BUBLIK_OUTBOX_CTID);
        chunkInsert.setLong(1, getId());
        chunkInsert.setLong(2, getStart());
        chunkInsert.setLong(3, getEnd());
        chunkInsert.setLong(4, rows);
        chunkInsert.setString(5, getConfig().fromTaskName());
        chunkInsert.setString(6, getTargetTable().getSchemaName().toLowerCase());
        chunkInsert.setString(7, getTargetTable().getFinalTableName(false));
        long r = chunkInsert.executeUpdate();
        chunkInsert.close();
    }
}
