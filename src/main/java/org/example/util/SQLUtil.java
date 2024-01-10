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

    public static Map<String, String> getNeededTargetColumnsAndTypes(SQLStatement sqlStatement,
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

    public static String buildStartEndRowIdOfChunkStatement(List<SQLStatement> sqlStatements) {
        List<String> taskNames = new ArrayList<>();
        sqlStatements.forEach(sqlStatement -> taskNames.add(sqlStatement.fromTaskName()));
//        return "select chunk_id, start_rowid, end_rowid, start_id, end_id, task_name from user_parallel_execute_chunks where task_name = ? " +
        return "select rownum, chunk_id, start_rowid, end_rowid, start_id, end_id, task_name from (" +
                "select chunk_id, start_rowid, end_rowid, start_id, end_id, task_name from user_parallel_execute_chunks where task_name in ('" +
                 String.join("', '", taskNames) + "') " +
                "and status <> 'PROCESSED' " +
//                "order by chunk_id";
                "order by ora_hash(concat(task_name,start_rowid)) ) order by 1";
    }
}
