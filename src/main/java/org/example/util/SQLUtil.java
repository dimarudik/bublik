package org.example.util;

import org.example.model.SQLStatement;

import java.util.*;
import java.util.stream.Collectors;

public class SQLUtil {

/*
    public static String buildInsertStatement(SQLStatement sqlStatement) {
        return "INSERT INTO " + sqlStatement.toSchemaName() + "." + sqlStatement.toTableName() +
                " (" +
                String.join(", ", getNeededTargetColumns(sqlStatement)) +
                ") VALUES (" +
                sqlStatement.targetColumnTypes()
                        .stream()
                        .map(columnType -> "cast (? as " + columnType)
                        .collect(Collectors.joining("), ")) + "))";
    }
*/

/*
    protected static List<String> getNeededSourceColumns(SQLStatement sqlStatement) {
        return getFields(sqlStatement, sqlStatement.sourceColumns(), sqlStatement.excludedSourceColumns());
    }
*/

/*
    private static List<String> getNeededTargetColumns(SQLStatement sqlStatement) {
        return getFields(sqlStatement, sqlStatement.targetColumns(), sqlStatement.excludedTargetColumns());
    }
*/

/*
    private static List<String> getFields(SQLStatement sqlStatement, List<String> columns, List<String> excludedColumns) {
        if (excludedColumns != null) {
            columns.removeAll(excludedColumns);
        }
        if (sqlStatement.targetColumnTypes() != null && !sqlStatement.targetColumnTypes().isEmpty()) {
            sqlStatement.setTargetColumnTypes(sqlStatement.targetColumnTypes().subList(0, columns.size()));
        }
        return columns;
    }
*/

    public static String buildCopyStatement(SQLStatement sqlStatement, Map<String, String> columnsToDB) {
        List<String> neededTargetColumns = new ArrayList<>(columnsToDB.keySet());
        if (sqlStatement.excludedTargetColumns() != null) {
            Set<String> excludedTargetColumns = new HashSet<>(sqlStatement.excludedTargetColumns());
            Map<String, String> neededTargetColumnsMap = new TreeMap<>(columnsToDB);
            neededTargetColumnsMap.keySet().removeAll(excludedTargetColumns);
            neededTargetColumns = new ArrayList<>(neededTargetColumnsMap.keySet());
        }
        return "COPY " + sqlStatement.toSchemaName() + "." + sqlStatement.toTableName() +
                " (" +
                String.join(", ", neededTargetColumns) +
                ") FROM STDIN";
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
