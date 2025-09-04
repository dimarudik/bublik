package org.bublik.core.util;

import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.model.Config;
import org.bublik.core.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

import static org.bublik.core.constants.SQLConstants.*;

public class ColumnUtil {
    private static final Logger log = LoggerFactory.getLogger(ColumnUtil.class);


    public static byte[] convertBlobToBytes(ResultSet resultSet, int i) throws SQLException {
        Blob blob = resultSet.getBlob(i);
        return getBlobBytes(blob);
    }

    public static byte[] convertBlobToBytes(ResultSet resultSet, String columnName) throws SQLException {
        Blob blob = resultSet.getBlob(columnName);
        return getBlobBytes(blob);
    }

    private static byte[] getBlobBytes(Blob blob) throws SQLException {
        return blob.getBytes(1, (int) blob.length());
    }

    public static String convertClobToString(ResultSet resultSet, int i) throws SQLException {
        Clob clob = resultSet.getClob(i);
        return getClobString(clob);
    }

    public static String convertClobToString(ResultSet resultSet, String columnName) throws SQLException {
        Clob clob = resultSet.getClob(columnName);
        return getClobString(clob);
    }

    private static String getClobString(Clob clob) throws SQLException {
        return clob.getSubString(1L, (int) clob.length());
    }

    public static int getColumnIndexByColumnName(ResultSet resultSet, String columnName) throws SQLException {
        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            if (columnName.equals(resultSet.getMetaData().getColumnName(i))) {
                return i;
            }
        }
        return 0;
    }

    public static Long getTotalPagesOfTable(Connection connection, Table table) throws SQLException {
        long heap_blks_total = 0;
        PreparedStatement preparedStatement = connection.prepareStatement(SQL_HEAP_BLKS_TOTAL);
        preparedStatement.setString(1, table.getSchemaName().toLowerCase() + "." +
                table.getTableName());
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            heap_blks_total = resultSet.getLong("heap_blks_total");
        }
        resultSet.close();
        preparedStatement.close();
        return heap_blks_total;
    }

    public static Long getMaxEndPageOfChunks(Connection connection, Config config) throws SQLException {
        long max_end_page = 0;
        PreparedStatement preparedStatement = connection.prepareStatement(SQL_MAX_END_PAGE);
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
                                            long last_id,
                                           long xidmin) throws SQLException {
        String sql = DML_BATCH_INSERT_CTID_CHUNKS_V2
                .replace("$schemaName", table.getSchemaName().toLowerCase())
                .replace("$tableName", table.getTableName());
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
        chunkInsert.setLong(9, last_id);
        chunkInsert.setLong(10, xidmin);
        chunkInsert.setLong(11, startPage);
        chunkInsert.setLong(12, totalPages);
        chunkInsert.setLong(13, pagesInChunk);
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
