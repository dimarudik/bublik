package dev.bublik.postgres.util;

import de.bytefish.pgbulkinsert.pgsql.model.interval.Interval;
import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.Table;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

import static java.lang.Byte.toUnsignedInt;
import static dev.bublik.postgres.constants.SQLConstants.*;

public class ColumnUtil {
    private static final Logger log = LoggerFactory.getLogger(ColumnUtil.class);

    private static final int HIGH_BIT_FLAG = 0x80000000;

    public static Interval byteArrayYMToInterval(byte[] bytes) {
        int year = toUnsignedInt(bytes[0]) << 24
                | toUnsignedInt(bytes[1]) << 16
                | toUnsignedInt(bytes[2]) << 8
                | toUnsignedInt(bytes[3]);
        year ^= HIGH_BIT_FLAG;
        int month = toUnsignedInt(bytes[4]) - 60;
        return new Interval(year * 12 + month, 0,0);
    }

    public static Interval byteArrayDSToInterval(byte[] bytes) {
        int day = toUnsignedInt(bytes[0]) << 24
                | toUnsignedInt(bytes[1]) << 16
                | toUnsignedInt(bytes[2]) << 8
                | toUnsignedInt(bytes[3]);
        day ^= HIGH_BIT_FLAG;
        int hour = toUnsignedInt(bytes[4]) - 60;
        int minute = toUnsignedInt(bytes[5]) - 60;
        int second = toUnsignedInt(bytes[6]) - 60;
        int nano = toUnsignedInt(bytes[7]) << 24
                | toUnsignedInt(bytes[8]) << 16
                | toUnsignedInt(bytes[9]) << 8
                | toUnsignedInt(bytes[10]);
        nano ^= HIGH_BIT_FLAG;
        return new Interval(0,
                day,
                hour,
                minute,
                second,
                nano / 1000);
    }

    public static LogSequenceNumber getCurrentLSN(Connection sqlConnection) throws SQLException {
        try (Statement st = sqlConnection.createStatement();
             ResultSet rs = st.executeQuery(SQL_PG_CURRENT_LSN_AND_XID)) {
            if (rs.next()) {
                String lsn = rs.getString(1);
                System.out.println(lsn);
                return LogSequenceNumber.valueOf(lsn);
            } else {
                return LogSequenceNumber.INVALID_LSN;
            }
        }
    }

    public static Long getTotalPagesOfTable(Connection connection, Table table) throws SQLException {
        long heap_blks_total = 0;
        PreparedStatement preparedStatement = connection.prepareStatement(SQL_HEAP_BLKS_TOTAL);
        preparedStatement.setString(1, table.getSchemaName().toLowerCase() + "." +
                table.getTableName());
//        log.info("Executing query: {}", preparedStatement);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            heap_blks_total = resultSet.getLong("heap_blks_total");
        }
        resultSet.close();
        preparedStatement.close();
        return heap_blks_total;
    }

    public static Long getMaxEndPageOfChunks(Connection connection, Config config, String chunkTableName) throws SQLException {
        long max_end_page = 0;
        PreparedStatement preparedStatement = connection.prepareStatement(SQL_MAX_END_PAGE.replace("$tableName", chunkTableName));
        preparedStatement.setString(1, config.fromTaskName());
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            max_end_page = resultSet.getLong("max_end_page");
        }
        resultSet.close();
        preparedStatement.close();
        return max_end_page;
    }

    public static void insertCtidChunksV2 (Connection connection,
                                           Config config,
                                           Table table,
                                           long startPage,
                                           long totalPages,
                                           long pagesInChunk,
                                           ChunkStatus status,
                                           int required,
                                           String chunkTableName) throws SQLException {
        String sql = DML_BATCH_INSERT_CHUNK_TABLE
                .replace("$tableName", chunkTableName);
//        ObjectMapper objectMapper = new ObjectMapper();
//        String jacksonData = objectMapper.writeValueAsString(config);
        PreparedStatement chunkInsert = connection.prepareStatement(sql);
        chunkInsert.setLong(1, pagesInChunk);
        chunkInsert.setLong(2, 0);
        chunkInsert.setString(3, config.fromTaskName());
        chunkInsert.setString(4, table.getSchemaName());
        chunkInsert.setString(5, table.getFinalTableName(true));
        chunkInsert.setString(6, status.toString());
        chunkInsert.setString(7, null);
        chunkInsert.setLong(8, required);
//        chunkInsert.setLong(9, last_id);
//        chunkInsert.setLong(10, xidmin);
        chunkInsert.setLong(9, startPage);
        chunkInsert.setLong(10, totalPages);
        chunkInsert.setLong(11, pagesInChunk);
        int rows = chunkInsert.executeUpdate();
        chunkInsert.close();
    }

/*
    private static void fillRowsStat(List<Config> configs, Connection initialConnection) throws SQLException {
        Statement statement = initialConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(SQL_CHUNKS);
        while(resultSet.next()) {
            int chunk_id = resultSet.getInt("chunk_id");
            long start_page = resultSet.getLong("start_page");
            long end_page = resultSet.getLong("end_page");
            PreparedStatement rowCountSQL = initialConnection.prepareStatement(
                    SQL_NUMBER_OF_TUPLES_PER_CHUNK_P1 +
                            SQL_NUMBER_OF_TUPLES_PER_CHUNK_P2);
            rowCountSQL.setLong(1, start_page);
            rowCountSQL.setLong(2, end_page);
            ResultSet set = rowCountSQL.executeQuery();
            while(set.next()) {
                long rows = set.getLong("rows");
                PreparedStatement updateRowsOfCtid = initialConnection.prepareStatement(DML_UPDATE_CTID_CHUNKS);
                updateRowsOfCtid.setLong(1, rows);
                updateRowsOfCtid.setInt(2, chunk_id);
                updateRowsOfCtid.execute();
                updateRowsOfCtid.close();
            }
            set.close();
            rowCountSQL.close();
            initialConnection.commit();
        }
        resultSet.close();
        statement.close();
    }
*/
}
