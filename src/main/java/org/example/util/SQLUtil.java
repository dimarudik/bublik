package org.example.util;

import org.example.model.SQLStatement;

import java.util.*;
import java.util.stream.Collectors;

public class SQLUtil {

    public static String buildCopyStatement(SQLStatement sqlStatement, Map<String, String> columnsToDB) {
        List<String> neededTargetColumns =
                new ArrayList<>(getNeededTargetColumnsAndTypes(sqlStatement, columnsToDB).keySet());
        return "COPY " + sqlStatement.toSchemaName() + "." + sqlStatement.toTableName() +
                " (" +
                String.join(", ", neededTargetColumns) +
                ") FROM STDIN";
    }

    public static String buildInsertStatement(SQLStatement sqlStatement, Map<String, String> columnsToDB) {
        List<String> neededTargetColumns =
                new ArrayList<>(getNeededTargetColumnsAndTypes(sqlStatement, columnsToDB).keySet());
        List<String> neededTargetTypes =
                new ArrayList<>(getNeededTargetColumnsAndTypes(sqlStatement, columnsToDB).values());
        return "INSERT INTO " + sqlStatement.toSchemaName() + "." + sqlStatement.toTableName() +
                " (" +
                String.join(", ", neededTargetColumns) +
                ") VALUES (" +
                neededTargetTypes
                        .stream()
                        .map(columnType -> "cast (? as " + columnType)
                        .collect(Collectors.joining("), ")) + "))";
    }

    private static Map<String, String> getNeededTargetColumnsAndTypes(SQLStatement sqlStatement,
                                                                      Map<String, String> columnsToDB) {
        Map<String, String> neededTargetColumnsMap = new TreeMap<>(columnsToDB);
        if (sqlStatement.excludedTargetColumns() != null) {
            Set<String> excludedTargetColumns = new HashSet<>(sqlStatement.excludedTargetColumns());
            neededTargetColumnsMap.keySet().removeAll(excludedTargetColumns);
        }
        return neededTargetColumnsMap;
    }

    public static String buildSQLFetchStatement(SQLStatement sqlStatement, Map<String, Integer> columnsFromDB) {
        List<String> neededSourceColumns = new ArrayList<>(columnsFromDB.keySet());
        if (sqlStatement.excludedSourceColumns() != null) {
            Set<String> excludedSourceColumns = new HashSet<>(sqlStatement.excludedSourceColumns());
            Map<String, Integer> neededSourceColumnsMap = new TreeMap<>(columnsFromDB);
            neededSourceColumnsMap.keySet().removeAll(excludedSourceColumns);
            neededSourceColumns = new ArrayList<>(neededSourceColumnsMap.keySet());
        }
        if (sqlStatement.numberColumn() == null) {
            return "select " +
                    sqlStatement.fetchHintClause() +
                    String.join(", ", neededSourceColumns) +
                    " from " +
                    sqlStatement.fromSchemaName() +
                    "." +
                    sqlStatement.fromTableName() +
                    " where " +
                    sqlStatement.fetchWhereClause() +
                    " and rowid between ? and ?";
        } else {
            return "select " +
                    sqlStatement.fetchHintClause() +
                    String.join(", ", neededSourceColumns) +
                    " from " +
                    sqlStatement.fromSchemaName() +
                    "." +
                    sqlStatement.fromTableName() +
                    " where " +
                    sqlStatement.fetchWhereClause() +
                    " and " + sqlStatement.numberColumn() + " between ? and ?";
        }
    }

    public static String buildStartEndRowIdOfChunkStatement() {
        return "select chunk_id, start_rowid, end_rowid, start_id, end_id from user_parallel_execute_chunks where task_name = ? " +
                "and status <> 'PROCESSED' " +
                //"and rownum <= 25 " +
                "order by chunk_id";
    }

/*
    public static String buildStartEndIdOfChunkStatement() {
        return "select chunk_id, start_id, end_id from user_parallel_execute_chunks where task_name = ? " +
                "order by chunk_id";
    }
*/
}
