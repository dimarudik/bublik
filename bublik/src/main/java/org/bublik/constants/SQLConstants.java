package org.bublik.constants;

public abstract class SQLConstants {
    public static final String LABEL_ORACLE = SourceContext.Oracle.toString();
    public static final String LABEL_POSTGRESQL = SourceContext.PostgreSQL.toString();
    public static final String SQL_MIN_MAX_BLOCK_NUMBER =
            "select " +
                    "(min(ctid)::text::point)[0]::bigint AS block_min, " +
                    "(max(ctid)::text::point)[0]::bigint + 1 AS block_max " +
                    "from ";
    public static final String SQL_NUMBER_OF_TUPLES =
            "select reltuples, relpages from pg_class " +
            "where relnamespace::regnamespace::text = ? and relname = ?";
    public static final String SQL_NUMBER_OF_RAW_TUPLES =
            "select pg_relation_size( ? ) / 8192 as heap_blks_total";
    public static final String SQL_MAX_END_PAGE =
            "select max(end_page) as max_end_page from public.ctid_chunks where task_name = ?";
    public static final String DDL_CREATE_POSTGRESQL_TABLE_CHUNKS =
            "create table if not exists public.ctid_chunks (" +
            "chunk_id int generated always as identity primary key, " +
            "start_page bigint, " +
            "end_page bigint, " +
            "task_name varchar(128), " +
            "status varchar(20)  default 'UNASSIGNED', " +
            "unique (start_page, end_page, task_name, status) )";
    public static final String DML_INSERT_CTID_CHUNKS =
            "insert into public.ctid_chunks (start_page, end_page, task_name) " +
            "values (?, ?, ?)";
    public static final String DML_BATCH_INSERT_CTID_CHUNKS =
            "insert into public.ctid_chunks (start_page, end_page, task_name) " +
            "(select id start_page, id + ? end_page, ? task_name " +
            "from generate_series(0, ?, ?) as id)";
    public static final String DML_UPDATE_STATUS_ROWID_CHUNKS = "CALL DBMS_PARALLEL_EXECUTE.SET_CHUNK_STATUS(?,?,2)";
    public static final String DML_UPDATE_STATUS_CTID_CHUNKS =
            "update public.ctid_chunks set status = 'PROCESSED' where chunk_id = ? and task_name = ?";
}
