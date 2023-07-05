package org.example.util;

import org.example.model.SQLStatement;

import java.util.stream.Collectors;

public class SQLUtil {
    public static String buildInsertStatement(SQLStatement sqlStatement) {
        return "INSERT INTO " + sqlStatement.getToSchemaName() + "." + sqlStatement.getToTableName() +
                " (" +
                String.join(", ", sqlStatement.getNeededTargetColumns()) +
                ") VALUES (" +
                sqlStatement.getTargetColumnTypes()
                        .stream()
                        .map(columnType -> "cast (? as " + columnType)
                        .collect(Collectors.joining("), ")) + "))";
    }

    public static String buildSQLFetchStatement(SQLStatement sqlStatement) {
        if (sqlStatement.getNeededSourceColumns() != null) {
            if (sqlStatement.getNumberColumn() == null) {
                return "select " +
                        sqlStatement.getFetchHintClause() +
                        String.join(", ", sqlStatement.getNeededSourceColumns()) +
                        " from " +
                        sqlStatement.getFromSchemaName() +
                        "." +
                        sqlStatement.getFromTableName() +
                        " where " +
                        sqlStatement.getFetchWhereClause() +
                        " and rowid between ? and ?";
            } else {
//                System.out.println(sqlStatement.getNumberColumn());
                return "select " +
                        sqlStatement.getFetchHintClause() +
                        String.join(", ", sqlStatement.getNeededSourceColumns()) +
                        " from " +
                        sqlStatement.getFromSchemaName() +
                        "." +
                        sqlStatement.getFromTableName() +
                        " where " +
                        sqlStatement.getFetchWhereClause() +
                        " and " + sqlStatement.getNumberColumn() + " between ? and ?";
            }
        } else {
            throw new RuntimeException("The list of source table columns is empty.");
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
