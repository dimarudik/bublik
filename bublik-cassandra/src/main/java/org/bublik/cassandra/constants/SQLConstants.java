package org.bublik.cassandra.constants;

public abstract class SQLConstants {
    public static final String DDL_CREATE_OUTBOX_TABLE =
            "create table $tableName (" +
                "chunk_id int, " +
                "task_name varchar, " +
                "rows int, " +
                "primary key (chunk_id))";
    public static final String DDL_DROP_OUTBOX_TABLE =
            "drop table $tableName";
    public static final String DML_INSERT_OUTBOX_TABLE =
            "insert into $tableName (chunk_id, task_name, rows) " +
                    "values (?, ?, ?)";
    public static final String DML_SELECT_OUTBOX_TABLE =
            "select chunk_id, task_name, rows from $tableName where chunk_id = ?";
}
