package org.bublik.ydb.constants;

public abstract class SQLConstants {
    public static final String DDL_DROP_OUTBOX_TABLE =
            "drop table $tableName_outbox";
    public static final String DDL_CREATE_OUTBOX_TABLE =
            "create table if not exists $tableName_outbox (" +
                    "chunk_id Uint32, " +
//                    "uuid String, " +
//                    "start_rowid String, " +
//                    "end_rowid String, " +
//                    "start_page Uint64, " +
//                    "end_page Uint64, " +
//                    "schema_name String, " +
//                    "table_name String, " +
                    "task_name String," +
                    "rows Uint64, " +
                    "primary key(chunk_id))";
    public static final String DML_INSERT_OUTBOX_TABLE =
            "insert into $tableName_outbox (chunk_id, task_name, rows) " +
                    "values (?, ?, ?)";
}
