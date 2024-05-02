package org.example.util;

import lombok.extern.slf4j.Slf4j;
import org.example.model.Config;
import org.example.model.OraChunk;
import org.example.model.PGChunk;
import org.example.model.PGColumn;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.example.constants.SQLConstants.*;
import static org.example.util.SQLUtil.buildStartEndPageOfPGChunk;
import static org.example.util.SQLUtil.buildStartEndRowIdOfOracleChunk;

@Slf4j
public class ColumnUtil {
//    private static final Logger log = LoggerFactory.getLogger(ColumnUtil.class);

    public static Map<String, PGColumn> readTargetColumnsAndTypes(Connection connection, Config config) {
        Map<String, PGColumn> columnMap = new HashMap<>();
        ResultSet resultSet;
        try {
            resultSet = connection.getMetaData().getColumns(
                    null,
                    config.toSchemaName().toLowerCase(),
                    config.toTableName().toLowerCase(),
                    null
            );
            Map<String, String> fromConfig = config.columnToColumn();
            while (resultSet.next()) {
                String columnName = resultSet.getString(4);
                String columnType = resultSet.getString(6);

                for (Map.Entry<String, String> entry : fromConfig.entrySet()) {
                    if (entry.getValue().replaceAll("\"","").equalsIgnoreCase(columnName)) {
                        columnMap.put(entry.getKey(), new PGColumn(entry.getValue(), columnType.equals("bigserial") ? "bigint" : columnType));
                    }
                }

            }
            resultSet.close();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return columnMap;
    }


/*
    public static Map<Integer, ColumnRule> getColumn2RuleMap(SQLStatement sqlStatement) {
        Map<Integer, ColumnRule> ruleMap = new HashMap<>();
        int i = 1;
        for (String fieldName: SQLUtil.getNeededSourceColumns(sqlStatement)) {
            Optional<ColumnRule> columnRule = Optional.empty();
            if (sqlStatement.transformToRule() != null) {
                columnRule =
                        sqlStatement.transformToRule()
                                .stream()
                                .filter(rule -> rule.getColumnName().equalsIgnoreCase(fieldName))
                                .findFirst();
            }
            assert Objects.requireNonNull(columnRule).isPresent();
            ruleMap.put(i, columnRule.orElse(null));
            i++;
        }
        return ruleMap;
    }
*/

    public static Map<Integer, OraChunk> getStartEndRowIdMap(Connection connection, List<Config> configs) {
        Map<Integer, OraChunk> chunkHashMap = new TreeMap<>();
        try {
            String sql = buildStartEndRowIdOfOracleChunk(configs);
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
/*
            if (!resultSet.isBeforeFirst()) {
                log.warn("No unprocessed chunks found in USER_PARALLEL_EXECUTE_CHUNKS for : " +
                        configs.stream().map(Config::fromTaskName).collect(Collectors.joining(", ")));
            } else {
*/
                while (resultSet.next()) {
                    chunkHashMap.put(resultSet.getInt(1),
                            new OraChunk(
                                    resultSet.getInt(2),
                                    resultSet.getString(3),
                                    resultSet.getString(4),
                                    resultSet.getLong(5),
                                    resultSet.getLong(6),
                                    findByTaskName(configs, resultSet.getString(7))
                            )
                    );
                }
//            }
            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return chunkHashMap;
    }

    public static Map<Integer, PGChunk> getStartEndCTIDMap(Connection connection, List<Config> configs) {
        Map<Integer, PGChunk> chunkHashMap = new TreeMap<>();
        try {
            String sql = buildStartEndPageOfPGChunk(configs);
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            if (!resultSet.isBeforeFirst()) {
                log.error("No chunk definition found in CTID_CHUNKS for : " +
                        configs.stream().map(Config::fromTaskName).collect(Collectors.joining(", ")));
            } else while (resultSet.next()) {
                chunkHashMap.put(resultSet.getInt("rownum"),
                        new PGChunk(
                                resultSet.getInt("chunk_id"),
                                resultSet.getLong("start_page"),
                                resultSet.getLong("end_page"),
                                findByTaskName(configs, resultSet.getString("task_name"))
                        )
                );
            }
//            chunkHashMap.forEach((k,v) -> System.out.println(k + " : " + v));
            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
//            logger.error(e.getMessage(), e);
        }
        return chunkHashMap;
    }

    public static void fillPGChunks(Connection connection, List<Config> configs) {
        try {
            Statement createTable = connection.createStatement();
            createTable.executeUpdate(DDL_CREATE_POSTGRESQL_TABLE_CHUNKS);
            configs.forEach(sqlStatement -> {
                try {
                    long reltuples = 0;
                    long relpages = 0;
                    PreparedStatement preparedStatement = connection.prepareStatement(SQL_NUMBER_OF_TUPLES);
                    preparedStatement.setString(1, sqlStatement.fromSchemaName().toLowerCase());
                    preparedStatement.setString(2, sqlStatement.fromTableName().toLowerCase());
                    ResultSet resultSet = preparedStatement.executeQuery();
                    while(resultSet.next()) {
                        reltuples = resultSet.getLong("reltuples");
                        relpages = resultSet.getLong("relpages");
                    }
                    double rowsInChunk = reltuples >= 500000 ? 100000d : 10000d;
                    long v = reltuples <= 0 && relpages <= 1 ? relpages + 1 :
                            (int) Math.round(relpages / (reltuples / rowsInChunk));
                    long pagesInChunk = Math.min(v, relpages + 1);
                    log.info("{}.{} \t\t\t relpages : {}\t reltuples : {}\t rowsInChunk : {}\t pagesInChunk : {} ",
                            sqlStatement.fromSchemaName(),
                            sqlStatement.fromTableName(),
                            relpages,
                            reltuples,
                            rowsInChunk,
                            pagesInChunk);
                    PreparedStatement chunkInsert = connection.prepareStatement(DML_BATCH_INSERT_CTID_CHUNKS);
                    chunkInsert.setLong(1, pagesInChunk);
                    chunkInsert.setString(2, sqlStatement.fromTaskName());
                    chunkInsert.setLong(3, relpages);
                    chunkInsert.setLong(4, pagesInChunk);
                    int rows = chunkInsert.executeUpdate();
                    resultSet.close();
                    preparedStatement.close();
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }
            });
            createTable.close();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }

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
