package org.bublik.cassandra.constants;

public abstract class SQLConstants {
    public static final String DDL_CREATE_CHUNK_TABLE =
            "create table $tableName (" +
                    "chunk_id TIMEUUID, " +
                    "start_page bigint, " +
                    "end_page bigint, " +
                    "task_name varchar, " +
                    "schema_name varchar, " +
                    "table_name varchar, " +
                    "required int, " +
                    "copied int, " +
                    "status varchar, " +
                    "start_ts timestamp, " +
                    "end_ts timestamp, " +
                    "err_msg text, " +
                    "primary key (status, chunk_id))";
    public static final String DML_INSERT_CHUNK_TABLE =
            "INSERT INTO $tableName (chunk_id, start_page, end_page, schema_name, table_name, status, task_name) VALUES (now(), ?, ?, ?, ?, ?, ?)";
    public static final String DML_INSERT_CHUNK_TABLE_WITH_ERR =
            "INSERT INTO $tableName (chunk_id, start_page, end_page, schema_name, table_name, status, task_name, err_msg) VALUES (now(), ?, ?, ?, ?, ?, ?, ?)";
    public static final String DML_DELETE_CHUNK_BY_ID =
            "delete from $tableName where chunk_id = ? and status = ?";
//    public static final String DML_UPDATE_STATUS_CHUNK_TABLE =
//            "update $tableName set status = ?, err_msg = null where chunk_id = ? and status = 'UNASSIGNED'";
    public static final String DML_UPDATE_STATUS_CHUNK_TABLE_WITH_ERRORS =
            "update $tableName set status = ?, err_msg = ? where chunk_id = ? and status = 'UNASSIGNED'";
    public static final String DDL_CREATE_OUTBOX_TABLE =
            "create table $tableName (" +
                "chunk_id int, " +
                "task_name varchar, " +
                "rows int, " +
                "primary key (chunk_id))";
    public static final String DDL_DROP_TABLE =
            "drop table $tableName";
    public static final String DML_INSERT_OUTBOX_TABLE =
            "insert into $tableName (chunk_id, task_name, rows) " +
                    "values (?, ?, ?)";
    public static final String DML_SELECT_OUTBOX_TABLE =
            "select chunk_id, task_name, rows from $tableName where chunk_id = ?";
    public static final String SQL_KEY_BY_TYPE =
            "select column_name, type, position from system_schema.columns " +
                    "where keyspace_name = ? and table_name = ? and kind = ? allow filtering";
}
