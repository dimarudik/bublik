package org.bublik.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bublik.constants.ChunkStatus;
import org.bublik.constants.PGKeywords;
import org.bublik.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.bublik.constants.SQLConstants.*;
import static org.bublik.exception.Utils.getStackTrace;

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
    public Chunk<?> saveChunkUpserted() throws SQLException {
        Connection connection = this.getSourceConnection();
        PreparedStatement updateStatus;
        updateStatus = connection.prepareStatement(DML_UPDATE_UPSERTED_CTID_CHUNKS);
        updateStatus.setInt(1, getUpserted());
        updateStatus.setInt(2, getUpserted());
        updateStatus.setInt(3, this.getId());
        int n = updateStatus.executeUpdate();
        updateStatus.close();
//        connection.commit();
        return this;
    }

    @Override
    public Chunk<?> saveConfig(boolean sync) throws SQLException {
//        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
//        mapper.writeValue(Paths.get(outputFileName).toFile(), configs);
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

    @Override
    public ResultSet getData(Connection connection, String query) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setLong(1, this.getStart());
        statement.setLong(2, this.getEnd());
        statement.setFetchSize(10000);
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

    public Map.Entry<Long, Long> getXidMinMax() throws SQLException {
        Config config = getConfig();
        String sql = SQL_SELECT_MAX_XMIN_XMAX_OF_CHUNK
                .replace("$schemaName", config.fromSchemaName())
                .replace("$tableName", config.fromTableName());
        PreparedStatement selectMaxXmin = getSourceConnection().prepareStatement(sql);
        selectMaxXmin.setLong(1, getStart());
        selectMaxXmin.setLong(2, getEnd());
        selectMaxXmin.setLong(3, getStart());
        selectMaxXmin.setLong(4, getEnd());
        ResultSet set = selectMaxXmin.executeQuery();
        long xidmin = 0;
        long xidmax = 0;
        while (set.next()) {
            xidmin = set.getLong("xidmin");
            xidmax = set.getLong("xidmax");
        }
        set.close();
        selectMaxXmin.close();
        return Map.entry(xidmin, xidmax);
    }

    public void insertParentChunk(Long xidMin)
            throws SQLException, JsonProcessingException {
        PreparedStatement ps = getSourceConnection().prepareStatement(DML_INSERT_CTID_CHUNKS);
        ObjectMapper objectMapper = new ObjectMapper();
        String jacksonData = objectMapper.writeValueAsString(getConfig());
        ps.setInt(1, getId());
        ps.setLong(2, getStart());
        ps.setLong(3, getEnd());
        ps.setLong(4, xidMin);
        ps.setLong(5, getXidMax());
        ps.setString(6, getConfig().fromTaskName());
        ps.setString(7, getSourceTable().getSchemaName());
        ps.setString(8, getSourceTable().getTableName());
        ps.setString(9, jacksonData);
        ps.setString(10, ChunkStatus.PROCESSED.toString());
        ps.setInt(11, 0);
        ps.executeUpdate();
        ps.close();
//        getSourceConnection().commit();
    }

    public String buildInsertOnConflictQuery() throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append(PGKeywords.INSERT).append(" ")
                .append(PGKeywords.INTO).append(" ")
                .append(getConfig().toSchemaName()).append(".").append(getConfig().toTableName()).append(" ")
                .append("(");
        Map<String, Column> columnMap = this.getTargetStorage().readTargetColumnsAndTypes(getTargetConnection(), this);
        sb.append(String.join(", ",
                columnMap
                        .values()
                        .stream()
                        .map(Column::getColumnName)
                        .toList()));
        sb.append(") ").append(PGKeywords.VALUES).append(" ");
        sb.append("(");
        sb.append(String.join(", ",
                columnMap
                        .values()
                        .stream()
                        .map(c -> "(?)::" + c.getColumnType())
                        .toList()));
//        sb.setLength(sb.length() - 2);
        sb.append(")");
        sb.append(" ON CONFLICT (");
        List<Column> pkColumns = getTargetTable().getPrimaryKeyColumns(getTargetConnection());
        if (pkColumns.isEmpty()) {
            throw new SQLException("No primary key columns found for table: " + getTargetTable().getTableName());
        }
        sb.append(
                pkColumns
                .stream()
                .map(Column::getColumnName)
                .collect(Collectors.joining(", ")));
        sb.append(") DO UPDATE SET ");
        sb.append(columnMap
                .values()
                .stream()
                .map(Column::getColumnName)
                .filter(columnName -> !pkColumns
                        .stream()
                        .map(Column::getColumnName)
                        .collect(Collectors.toSet())
                        .contains(columnName))
                .map(p -> p + " = EXCLUDED." + p)
                .collect(Collectors.joining(", ")));
        return sb.toString();
    }

    public void upsertToTarget() throws SQLException {
        this
                .insertOnConflict()
                .saveChunkUpserted()
                .saveChunkStatus(getUpserted() > 0 ? ChunkStatus.SYNCED : ChunkStatus.UNCHANGED, false);
        if (getUpserted() > 0) {
            log.info("PostgreSQL UPSERT ChunkId = {}  taskName = {} Schema = {} Table = {} rows = {} xmin > {}",
                    getId(), getConfig().fromTaskName(), getConfig().fromSchemaName(),
                    getTargetTable().getTableName(), getUpserted(), getXidMin());
        }
    }

    public Chunk<?> insertOnConflict() {
        try {
            Connection fromConnection = getSourceConnection();
            Connection toConnection = getTargetConnection();
            PreparedStatement st = fromConnection.prepareStatement(getFetchQuery());
//            log.info("{}", getFetchQuery());
            st.setLong(1, getStart());
            st.setLong(2, getEnd());
            st.setLong(3, getXidMin());
            st.setLong(4, getXidMin());
            ResultSet rs = st.executeQuery();
            int upserted = 0;
            if (rs.isBeforeFirst()) {
                setBatchInsertQuery(buildInsertOnConflictQuery());
                PreparedStatement ps = toConnection.prepareStatement(getBatchInsertQuery());
                while (rs.next()) {
                    Map<String, Column> columnMap = this.getTargetStorage().readTargetColumnsAndTypes(toConnection, this);
                    int index = 1;
                    for (Map.Entry<String, Column> entry : columnMap.entrySet()) {
                        String columnName = entry.getKey();
                        Column targetColumn = entry.getValue();
                        ps.setObject(index++, rs.getObject(columnName.replaceAll("\"", "")), targetColumn.getDataType());
                    }
                    ps.addBatch();
                    upserted++;
                }
                int[] n = ps.executeBatch();
                ps.close();
                toConnection.commit();
            }
            setUpserted(upserted);
            st.close();
            rs.close();
        } catch (SQLException e) {
            log.error("ChunkId = {}  {}", getId(), getStackTrace(e));
            throw new RuntimeException(e);
        }
        return this;
    }

    public int getHeapBlksTotal() {
        try {
            Connection connection = getSourceConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_HEAP_BLKS_TOTAL_SYNC);
            ps.setLong(1, getId());
            ResultSet rs = ps.executeQuery();
            int heapBlksTotal = 0;
            if (rs.next()) {
                heapBlksTotal = rs.getInt("heap_blks_total");
            }
            rs.close();
            ps.close();
            return heapBlksTotal;
        } catch (SQLException e) {
            log.error("Error getting heap blocks total for chunkId = {}: {}", getId(), getStackTrace(e));
            return 0;
        }
    }
}
