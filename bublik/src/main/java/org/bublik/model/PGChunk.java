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
import java.util.Map;
import java.util.stream.Collectors;

import static org.bublik.constants.SQLConstants.*;

public class PGChunk<T extends Long> extends Chunk<T> {
    private static final Logger log = LoggerFactory.getLogger(PGChunk.class);
    private final Integer parentId;
    private final Integer xidMin;
    private final Integer xidMax;

    public PGChunk(Integer id, T start, T end, Config config, Table sourceTable, String fetchQuery, Storage sourceStorage) {
        super(id, start, end, config, sourceTable, fetchQuery, sourceStorage);
        this.parentId = null;
        this.xidMin = null;
        this.xidMax = null;
    }

    public PGChunk(Integer id, T start, T end, Config config, Table sourceTable, Table targetTable, Storage sourceStorage,
                   Integer parentId, Integer xidMin, Integer xidMax, Connection sourceConnection, String fetchQuery) {
        super(id, start, end, config, sourceTable, fetchQuery, sourceStorage);
        this.parentId = parentId;
        this.xidMin = xidMin;
        this.xidMax = xidMax;
        this.setTargetTable(targetTable);
        this.setSourceConnection(sourceConnection);
    }

    public Integer getParentId() {
        return parentId;
    }

    public Integer getXidMin() {
        return xidMin;
    }

    public Integer getXidMax() {
        return xidMax;
    }

    @Override
    public PGChunk<T> saveChunkStatus(ChunkStatus status, Integer errNum, String errMsg) throws SQLException {
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
            connection.commit();
        }
//        LOGGER.debug("setChunkStatus {}", status);
        return this;
    }

    @Override
    public Chunk<?> saveChunkRows(int rows) throws SQLException {
        Connection connection = this.getSourceConnection();
        PreparedStatement updateStatus;
        updateStatus = connection.prepareStatement(DML_UPDATE_ROWS_CTID_CHUNKS);
        updateStatus.setInt(1, rows);
        updateStatus.setInt(2, this.getId());
        int n = updateStatus.executeUpdate();
        updateStatus.close();
        connection.commit();
        return this;
    }

    @Override
    public Chunk<?> saveChunkUpserted() throws SQLException {
        Connection connection = this.getSourceConnection();
        PreparedStatement updateStatus;
        updateStatus = connection.prepareStatement(DML_UPDATE_UPSERTED_CTID_CHUNKS);
        updateStatus.setInt(1, getUpserted());
        updateStatus.setInt(2, this.getId());
        int n = updateStatus.executeUpdate();
        updateStatus.close();
        connection.commit();
        return this;
    }

    @Override
    public Chunk<?> saveConfig() throws SQLException {
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

    public Map.Entry<Integer, Integer> getXidMinMax() throws SQLException {
        Config config = getConfig();
        String sql = SQL_SELECT_MAX_XMIN_XMAX_OF_CHUNK
                .replace("$schemaName", config.fromSchemaName())
                .replace("$tableName", config.fromTableName());
        PreparedStatement selectMaxXmin =
                getSourceConnection().prepareStatement(sql);
        selectMaxXmin.setLong(1, getStart());
        selectMaxXmin.setLong(2, getEnd());
        ResultSet set = selectMaxXmin.executeQuery();
        int xidmin = 0;
        int xidmax = 0;
        while (set.next()) {
            xidmin = set.getInt("xidmin");
            xidmax = set.getInt("xidmax");
        }
        set.close();
        selectMaxXmin.close();
        return Map.entry(xidmin, xidmax);
    }

    public void insertParentChunk(Integer xidMin)
            throws SQLException, JsonProcessingException {
        PreparedStatement ps = getSourceConnection().prepareStatement(DML_INSERT_CTID_CHUNKS);
        ObjectMapper objectMapper = new ObjectMapper();
        String jacksonData = objectMapper.writeValueAsString(getConfig());
        ps.setInt(1, getId());
        ps.setLong(2, getStart());
        ps.setLong(3, getEnd());
        ps.setInt(4, xidMin);
        ps.setInt(5, getXidMax());
        ps.setString(6, getConfig().fromTaskName());
        ps.setString(7, getSourceTable().getSchemaName());
        ps.setString(8, getSourceTable().getTableName());
        ps.setString(9, jacksonData);
        ps.setString(10, ChunkStatus.PROCESSED.toString());
        ps.setInt(11, 0);
        ps.executeUpdate();
        ps.close();
        getSourceConnection().commit();
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
        sb.setLength(sb.length() - 2); // Remove last comma and space
        sb.append(")");
        sb.append(" ON CONFLICT (");
        sb.append(getTargetTable().getPKColumns(getTargetConnection())
                .stream()
                .map(Column::getColumnName)
                .collect(Collectors.joining(", ")));
        sb.append(") DO UPDATE SET ");
        sb.append(columnMap
                .values()
                .stream()
                .map(Column::getColumnName)
                .filter(columnName -> {
                    try {
                        return !getTargetTable().getPKColumns(getTargetConnection())
                                .stream()
                                .map(Column::getColumnName)
                                .collect(Collectors.toSet())
                                .contains(columnName);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                })
                .map(p -> p + " = EXCLUDED." + p)
                .collect(Collectors.joining(", ")));
        return sb.toString();
    }

    public Chunk<?> insertOnConflict() throws SQLException {
        Connection fromConnection = getSourceConnection();
        Connection toConnection = getTargetConnection();
        PreparedStatement st = fromConnection.prepareStatement(getFetchQuery());
        st.setLong(1, getStart());
        st.setLong(2, getEnd());
        st.setInt(3, getXidMin());
//        log.info("{} {} {} {}", getFetchQuery(), getStart(), getEnd(), getXidMin());
        ResultSet rs = st.executeQuery();
//        log.info("{}", getBatchInsertQuery());
        PreparedStatement ps = toConnection.prepareStatement(getBatchInsertQuery());
        int upserted = 0;
        while (rs.next()) {
            Map<String, Column> columnMap = this.getTargetStorage().readTargetColumnsAndTypes(toConnection, this);
            int index = 1;
            for(Map.Entry<String, Column> entry : columnMap.entrySet()) {
                String columnName = entry.getKey();
                Column targetColumn = entry.getValue();
                ps.setObject(index++, rs.getObject(columnName.replaceAll("\"", "")), targetColumn.getDataType());
            }
            ps.addBatch();
            upserted++;
        }
        int[] n = ps.executeBatch();
        toConnection.commit();
        setUpserted(upserted);
        ps.close();
        st.close();
        rs.close();
        return this;
    }
}
