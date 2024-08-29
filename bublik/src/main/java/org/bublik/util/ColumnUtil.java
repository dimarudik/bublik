package org.bublik.util;

import de.bytefish.pgbulkinsert.pgsql.model.interval.Interval;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import org.bublik.Bublik;
import org.bublik.model.Config;
import org.bublik.model.Table;
import org.bublik.service.TableService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

import static java.lang.Byte.toUnsignedInt;
import static org.bublik.constants.SQLConstants.*;
import static org.bublik.exception.Utils.getStackTrace;

public class ColumnUtil {
    private static final int HIGH_BIT_FLAG = 0x80000000;
    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnUtil.class);

    public static Interval intervalYM2Interval(INTERVALYM intervalym) {
        byte[] bytes;
        bytes = intervalym.toBytes();
        int year = toUnsignedInt(bytes[0]) << 24
                | toUnsignedInt(bytes[1]) << 16
                | toUnsignedInt(bytes[2]) << 8
                | toUnsignedInt(bytes[3]);
        year ^= HIGH_BIT_FLAG;
        int month = toUnsignedInt(bytes[4]) - 60;
        return new Interval(year * 12 + month, 0,0);
    }

    public static Interval intervalDS2Interval(INTERVALDS intervalds) {
        byte[] bytes;
        bytes = intervalds.toBytes();
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

    public static void fillCtidChunks(List<Config> configs, Connection initialConnection) {
        createTableCtidChunks(initialConnection);
        try {
            for (Config config : configs) {
                long reltuples = 0;
                long relpages = 0;
                long max_end_page = 0;
                long heap_blks_total = 0;
                PreparedStatement preparedStatement = initialConnection.prepareStatement(SQL_NUMBER_OF_TUPLES);
                Table table = TableService.getTable(initialConnection, config.fromSchemaName(), config.fromTableName());
                preparedStatement.setString(1, table.getSchemaName().toLowerCase());
                preparedStatement.setString(2, table.getFinalTableName(false));
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    reltuples = resultSet.getLong("reltuples");
                    relpages = resultSet.getLong("relpages");
                }
                resultSet.close();
                preparedStatement.close();
                preparedStatement = initialConnection.prepareStatement(SQL_NUMBER_OF_RAW_TUPLES);
                preparedStatement.setString(1, table.getSchemaName().toLowerCase() + "." +
                        table.getTableName());
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    heap_blks_total = resultSet.getLong("heap_blks_total");
                }
                resultSet.close();
                preparedStatement.close();
                double rowsInChunk = reltuples >= 500000 ? ROWS_IN_CHUNK : 10000d;
//            double rowsInChunk = 10000d;
                long v = reltuples <= 0 && relpages <= 1 ? relpages + 1 :
                        (int) Math.round(relpages / (reltuples / rowsInChunk));
                long pagesInChunk = Math.min(v, relpages + 1);
                LOGGER.info("{}.{} \t\t\t relpages : {}\t heap_blks_total : {}\t reltuples : {}\t rowsInChunk : {}\t pagesInChunk : {} ",
                        config.fromSchemaName(),
                        config.fromTableName(),
                        relpages,
                        heap_blks_total,
                        reltuples,
                        rowsInChunk,
                        pagesInChunk);
                PreparedStatement chunkInsert = initialConnection.prepareStatement(DML_BATCH_INSERT_CTID_CHUNKS);
                chunkInsert.setLong(1, pagesInChunk);
                chunkInsert.setLong(2, 0);
                chunkInsert.setString(3, config.fromTaskName());
                chunkInsert.setString(4, table.getSchemaName().toLowerCase());
                chunkInsert.setString(5, table.getFinalTableName(false));
                chunkInsert.setLong(6, relpages);
                chunkInsert.setLong(7, pagesInChunk);
//            System.out.println(chunkInsert);
                int rows = chunkInsert.executeUpdate();
                chunkInsert.close();

                preparedStatement = initialConnection.prepareStatement(SQL_MAX_END_PAGE);
                preparedStatement.setString(1, config.fromTaskName());
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    max_end_page = resultSet.getLong("max_end_page");
                }
                resultSet.close();
                preparedStatement.close();
                if (heap_blks_total > max_end_page) {
                    chunkInsert = initialConnection.prepareStatement(DML_INSERT_CTID_CHUNKS);
                    chunkInsert.setLong(1, max_end_page);
                    chunkInsert.setLong(2, heap_blks_total);
                    chunkInsert.setLong(3, 0);
                    chunkInsert.setString(4, config.fromTaskName());
                    chunkInsert.setString(5, table.getSchemaName().toLowerCase());
                    chunkInsert.setString(6, table.getFinalTableName(false));
                    rows = chunkInsert.executeUpdate();
                    chunkInsert.close();
                }
            }
            initialConnection.commit();
        } catch (SQLException e) {
            LOGGER.error("{}", getStackTrace(e));
        }
    }

    private static void createTableCtidChunks(Connection initialConnection) {
        try {
            Statement createTable = initialConnection.createStatement();
            createTable.executeUpdate(DDL_CREATE_POSTGRESQL_TABLE_CHUNKS);
            createTable.close();
            Statement truncateTable = initialConnection.createStatement();
            truncateTable.executeUpdate(DDL_TRUNCATE_POSTGRESQL_TABLE_CHUNKS);
            truncateTable.close();
        } catch (SQLException e) {
            LOGGER.error("{}", getStackTrace(e));
        }
    }
}
