package org.bublik.ydb.constants;

public abstract class SQLConstants {
    public static final String DDL_DROP_YDB_TABLE_BUBLIK_OUTBOX =
            "drop table bublik_outbox;";
    public static final String DDL_CREATE_YDB_TABLE_BUBLIK_OUTBOX =
            "create table if not exists bublik_outbox (" +
                    "chunk_id Uint32, " +
                    "uuid String, " +
                    "start_rowid String, " +
                    "end_rowid String, " +
                    "start_page Uint64, " +
                    "end_page Uint64, " +
                    "schema_name String, " +
                    "table_name String, " +
                    "rows Uint64, " +
                    "task_name String," +
                    "primary key(chunk_id))";
}
