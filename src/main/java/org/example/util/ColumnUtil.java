package org.example.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.model.Chunk;
import org.example.model.SQLStatement;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.example.util.SQLUtil.buildStartEndRowIdOfChunkStatement;

public class ColumnUtil {
    private static final Logger logger = LogManager.getLogger(ColumnUtil.class);

    public static Map<String, Integer> readSourceColumnsFromDB(Connection connection, SQLStatement sqlStatement) {
        Map<String, Integer> columnMap = new TreeMap<>();
        ResultSet resultSet;
        try {
            resultSet = connection.getMetaData().getColumns(
                    null,
                    sqlStatement.fromSchemaName(),
                    sqlStatement.fromTableName().replaceAll("^\"|\"$", ""),
                    null
            );
            while (resultSet.next()) {
                String columnName = resultSet.getString(4);
                Integer columnType = Integer.valueOf(resultSet.getString(5));
                columnMap.put(columnName, columnType);
            }
            resultSet.close();
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        return columnMap;
    }

    public static Map<String, String> readTargetColumnsFromDB(Connection connection, SQLStatement sqlStatement) {
        Map<String, String> columnMap = new TreeMap<>();
        ResultSet resultSet;
        try {
            resultSet = connection.getMetaData().getColumns(
                    null,
                    sqlStatement.toSchemaName().toLowerCase(),
                    sqlStatement.toTableName().toLowerCase(),
                    null
            );
            while (resultSet.next()) {
                String columnName = resultSet.getString(4);
                String columnType = resultSet.getString(6);
                columnMap.put(columnName, columnType);
            }
            resultSet.close();
        } catch (SQLException e) {
            logger.error(e.getMessage());
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

    static HashMap<Integer, Chunk> getStartEndRowIdMap(Connection connection, SQLStatement sqlStatement) {
        HashMap<Integer, Chunk> chunkHashMap = new HashMap<>();
        try {
            PreparedStatement statement = connection.prepareStatement(buildStartEndRowIdOfChunkStatement());
            statement.setString(1, sqlStatement.fromTaskName());
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                chunkHashMap.put(resultSet.getInt(1),
                        new Chunk(
                                resultSet.getInt(1),
                                resultSet.getString(2),
                                resultSet.getString(3),
                                resultSet.getLong(4),
                                resultSet.getLong(5)
                        )
                );
            }
            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        return chunkHashMap;
    }

    public static byte[] convertBlobToBytes(ResultSet resultSet, int i) throws SQLException {
        Blob blob = resultSet.getBlob(i);
        int blobLength = (int) blob.length();
        return blob.getBytes(1, blobLength);
    }

    public static String convertClobToBytes(ResultSet resultSet, int i) throws SQLException {
        Clob clob = resultSet.getClob(i);
        int clobLength = (int) clob.length();
        return clob.getSubString(1L, clobLength);
    }
}
