package org.example.util;

import org.example.model.Config;

import java.util.*;

public class SQLUtil {

/*
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
*/

    public static Map<String, String> getNeededTargetColumnsAndTypes(Config config,
                                                                     Map<String, String> columnsToDB) {
        Map<String, String> neededTargetColumnsMap = new TreeMap<>(columnsToDB);
        if (config.excludedTargetColumns() != null) {
            Set<String> excludedTargetColumns = new HashSet<>(config.excludedTargetColumns());
            neededTargetColumnsMap.keySet().removeAll(excludedTargetColumns);
        }
        return neededTargetColumnsMap;
    }

/*
    public static String buildOraSQLFetchStatement(Config config, Map<String, Integer> columnsFromDB) {
        List<String> neededSourceColumns = new ArrayList<>(columnsFromDB.keySet());
        if (config.excludedSourceColumns() != null) {
            Set<String> excludedSourceColumns = new HashSet<>(config.excludedSourceColumns());
            Map<String, Integer> neededSourceColumnsMap = new TreeMap<>(columnsFromDB);
            neededSourceColumnsMap.keySet().removeAll(excludedSourceColumns);
            neededSourceColumns = new ArrayList<>(neededSourceColumnsMap.keySet());
        }
        if (config.numberColumn() == null) {
            return "select " +
                    config.fetchHintClause() +
                    String.join(", ", neededSourceColumns) +
                    " from " +
                    config.fromSchemaName() +
                    "." +
                    config.fromTableName() +
                    " where " +
                    config.fetchWhereClause() +
                    " and rowid between ? and ?";
        } else {
            return "select " +
                    config.fetchHintClause() +
                    String.join(", ", neededSourceColumns) +
                    " from " +
                    config.fromSchemaName() +
                    "." +
                    config.fromTableName() +
                    " where " +
                    config.fetchWhereClause() +
                    " and " + config.numberColumn() + " between ? and ?";
        }
    }
*/

/*
    public static String buildPGSQLFetchStatement(Config config,
                                                  Map<String, Integer> columnsFromDB,
                                                  Long page_start,
                                                  Long page_end) {
        List<String> neededSourceColumns = new ArrayList<>(columnsFromDB.keySet());
        if (config.excludedSourceColumns() != null) {
            Set<String> excludedSourceColumns = new HashSet<>(config.excludedSourceColumns());
            Map<String, Integer> neededSourceColumnsMap = new TreeMap<>(columnsFromDB);
            neededSourceColumnsMap.keySet().removeAll(excludedSourceColumns);
            neededSourceColumns = new ArrayList<>(neededSourceColumnsMap.keySet());
        }
        return "select " +
                String.join(", ", neededSourceColumns) +
                " from " +
                config.fromSchemaName() +
                "." +
                config.fromTableName() +
                " where " +
                config.fetchWhereClause() +
                " and ctid >= '(" + page_start +",1)' and ctid < '(" + page_end + ",1)'";
    }
*/

    public static String buildStartEndRowIdOfOracleChunk(List<Config> configs) {
        List<String> taskNames = new ArrayList<>();
        configs.forEach(sqlStatement -> taskNames.add(sqlStatement.fromTaskName()));
        return "select rownum, chunk_id, start_rowid, end_rowid, start_id, end_id, task_name from (" +
                "select chunk_id, start_rowid, end_rowid, start_id, end_id, task_name from user_parallel_execute_chunks where task_name in ('" +
                 String.join("', '", taskNames) + "') " +
                "and status <> 'PROCESSED' " +
                "order by ora_hash(concat(task_name,start_rowid)) ) order by 1";
    }

    public static String buildStartEndPageOfPGChunk(List<Config> configs) {
        List<String> taskNames = new ArrayList<>();
        configs.forEach(sqlStatement -> taskNames.add(sqlStatement.fromTaskName()));
        return "select row_number() over (order by chunk_id) as rownum, chunk_id, start_page, end_page, task_name from public.ctid_chunks where task_name in ('" +
                String.join("', '", taskNames) + "') " +
                "and status <> 'PROCESSED' ";
    }
}
