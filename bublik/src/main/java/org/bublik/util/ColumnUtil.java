package org.bublik.util;

import org.bublik.Bublik;
import org.bublik.model.*;
import org.bublik.service.TableService;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.bublik.constants.SQLConstants.*;
import static org.bublik.util.SQLUtil.buildStartEndPageOfPGChunk;
import static org.bublik.util.SQLUtil.buildStartEndRowIdOfOracleChunk;

public class ColumnUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(Bublik.class);

    public static Map<String, PGColumn> readTargetColumnsAndTypes(Connection connection, Chunk<?> chunk) {
        Map<String, PGColumn> columnMap = new HashMap<>();
        try {
            ResultSet resultSet;
            resultSet = connection.getMetaData().getColumns(
                    null,
                    chunk.getTargetTable().getSchemaName().toLowerCase(),
                    chunk.getTargetTable().getFinalTableName(false),
                    null);
            Map<String, String> columnToColumnMap = chunk.getConfig().columnToColumn();
            Map<String, String> expressionToColumnMap = chunk.getConfig().expressionToColumn();
            while (resultSet.next()) {
                String columnName = resultSet.getString(4);
                String columnType = resultSet.getString(6);
                Integer columnPosition = resultSet.getInt(17);

                columnToColumnMap.entrySet()
                        .stream()
                        .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(columnName))
                        .forEach(i -> columnMap.put(i.getKey(), new PGColumn(columnPosition, i.getValue(), columnType.equals("bigserial") ? "bigint" : columnType)));

                if (chunk.getConfig().expressionToColumn() != null) {
                    expressionToColumnMap.entrySet()
                            .stream()
                            .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(columnName))
                            .forEach(i -> columnMap.put(columnName, new PGColumn(columnPosition, i.getValue(), columnType.equals("bigserial") ? "bigint" : columnType)));
                }
            }
            resultSet.close();
        } catch (SQLException e) {
            System.out.println(e);
        }
        return columnMap;
    }

    public static <T> Map<Integer, Chunk<T>> getChunkMap(Connection connection, List<Config> configs) throws SQLException {
        if (connection.isWrapperFor(oracle.jdbc.OracleConnection.class)) {
            Map<Integer, Chunk<T>> chunkHashMap = new TreeMap<>();
            String sql = buildStartEndRowIdOfOracleChunk(configs);
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Config config = findByTaskName(configs, resultSet.getString("task_name"));
                assert config != null;
                chunkHashMap.put(resultSet.getInt("rownum"),
                        new OraChunk(
                                resultSet.getInt("chunk_id"),
                                resultSet.getRowId("start_rowid"),
                                resultSet.getRowId("end_rowid"),
                                config,
                                TableService.getTable(connection, config.fromSchemaName(), config.fromTableName()),
                                null
                        )
                );
            }
            resultSet.close();
            statement.close();
            return chunkHashMap;
        } else if (connection.isWrapperFor(PGConnection.class)) {
            Map<Integer, Chunk<T>> chunkHashMap = new TreeMap<>();
            String sql = buildStartEndPageOfPGChunk(configs);
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            if (!resultSet.isBeforeFirst()) {
                System.out.println("No chunk definition found in CTID_CHUNKS for : " +
                        configs.stream().map(Config::fromTaskName).collect(Collectors.joining(", ")));
            } else while (resultSet.next()) {
                Config config = findByTaskName(configs, resultSet.getString("task_name"));
                assert config != null;
                chunkHashMap.put(resultSet.getInt("rownum"),
                        new PGChunk(
                                resultSet.getInt("chunk_id"),
                                resultSet.getLong("start_page"),
                                resultSet.getLong("end_page"),
                                config,
                                TableService.getTable(connection, config.fromSchemaName(), config.fromTableName()),
                                null
                        )
                );
            }
            resultSet.close();
            statement.close();
            return chunkHashMap;
        }
        return null;
    }

    public static String fillCtidChunks(Connection connection, List<Config> configs) throws SQLException {
//        String message;
        Statement createTable = connection.createStatement();
        createTable.executeUpdate(DDL_CREATE_POSTGRESQL_TABLE_CHUNKS);
        createTable.close();
        connection.commit();
        for (Config config : configs) {
            long reltuples = 0;
            long relpages = 0;
            long max_end_page = 0;
            long heap_blks_total = 0;
            PreparedStatement preparedStatement = connection.prepareStatement(SQL_NUMBER_OF_TUPLES);
            Table table = TableService.getTable(connection, config.fromSchemaName(), config.fromTableName());
            preparedStatement.setString(1, table.getSchemaName().toLowerCase());
            preparedStatement.setString(2, table.getFinalTableName(false));
//            System.out.println(table.getSchemaName() + " " + table.getFinalTableName(false));
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next()) {
                reltuples = resultSet.getLong("reltuples");
                relpages = resultSet.getLong("relpages");
            }
            resultSet.close();
            preparedStatement.close();
            preparedStatement = connection.prepareStatement(SQL_NUMBER_OF_RAW_TUPLES);
            preparedStatement.setString(1, table.getSchemaName().toLowerCase() + "." +
                    table.getTableName());
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()) {
                heap_blks_total = resultSet.getLong("heap_blks_total");
            }
            resultSet.close();
            preparedStatement.close();
            double rowsInChunk = reltuples >= 500000 ? 100000d : 10000d;
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
            PreparedStatement chunkInsert = connection.prepareStatement(DML_BATCH_INSERT_CTID_CHUNKS);
            chunkInsert.setLong(1, pagesInChunk);
            chunkInsert.setString(2, config.fromTaskName());
            chunkInsert.setLong(3, relpages);
            chunkInsert.setLong(4, pagesInChunk);
            int rows = chunkInsert.executeUpdate();
            chunkInsert.close();
            preparedStatement = connection.prepareStatement(SQL_MAX_END_PAGE);
            preparedStatement.setString(1, config.fromTaskName());
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()) {
                max_end_page = resultSet.getLong("max_end_page");
            }
            resultSet.close();
            preparedStatement.close();
            if (heap_blks_total > max_end_page) {
                chunkInsert = connection.prepareStatement(DML_INSERT_CTID_CHUNKS);
                chunkInsert.setLong(1, max_end_page);
                chunkInsert.setLong(2, heap_blks_total);
                chunkInsert.setString(3, config.fromTaskName());
                rows = chunkInsert.executeUpdate();
                chunkInsert.close();
            }
        }
        connection.commit();
        return "";
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

    public static Config findByTaskName(List<Config> configs, String taskName) {
        for (Config config : configs) {
            if (config.fromTaskName().equals(taskName)) {
                return config;
            }
        }
        return null;
    }
}
