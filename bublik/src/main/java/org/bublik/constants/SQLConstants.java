package org.bublik.constants;

public abstract class SQLConstants {
//    public static final String LABEL_ORACLE = SourceContext.Oracle.toString();
//    public static final String LABEL_POSTGRESQL = SourceContext.PostgreSQL.toString();
    public static final double ROWS_IN_CHUNK = 100000d;
    public static final String SQL_MIN_MAX_BLOCK_NUMBER =
            "select " +
                    "(min(ctid)::text::point)[0]::bigint AS block_min, " +
                    "(max(ctid)::text::point)[0]::bigint + 1 AS block_max " +
                    "from ";
    public static final String SQL_NUMBER_OF_TUPLES =
            "select reltuples, relpages from pg_class " +
            "where relnamespace::regnamespace::text = ? and relname = ?";
    public static final String SQL_HEAP_BLKS_TOTAL =
            "select pg_relation_size( ? ) / 8192 as heap_blks_total";
    public static final String SQL_MAX_END_PAGE =
            "select max(end_page) as max_end_page from public.ctid_chunks where task_name = ?";
    public static final String DDL_DROP_PG_TABLE_CTID_CHUNKS =
            "drop table public.ctid_chunks;";
    public static final String DDL_TRUNCATE_PG_TABLE_CTID_CHUNKS =
            "truncate table public.ctid_chunks;";
    public static final String DDL_CREATE_PG_TABLE_CTID_CHUNKS =
            "create table if not exists public.ctid_chunks (" +
            "chunk_id int generated always as identity primary key, " +
            "parent_id int, " +
            "last_id int, " +
            "start_page bigint, " +
            "end_page bigint, " +
            "xidmin int8, " +
            "xidmax int8, " +
            "schema_name varchar(128), " +
            "table_name varchar(256), " +
            "config jsonb, " +
            "required bigint, " +
            "copied bigint, " +
            "upserted bigint, " +
            "task_name varchar(128), " +
            "status varchar(20)  default 'UNASSIGNED', " +
            "start_ts timestamp, " +
            "end_ts timestamp, " +
            "err_msg varchar(2048), " +
            "unique (xidmin, start_page, end_page, task_name, status) )";
    public static final String DDL_TRUNCATE_PG_TABLE_BUBLIK_OUTBOX =
            "truncate table public.bublik_outbox;";
    public static final String DDL_CREATE_PG_TABLE_BUBLIK_OUTBOX =
            "create table if not exists public.bublik_outbox (" +
            "chunk_id int primary key, " +
            "start_rowid varchar(32), " +
            "end_rowid varchar(32), " +
            "start_page bigint, " +
            "end_page bigint, " +
            "schema_name varchar(128), " +
            "table_name varchar(128), " +
            "rows bigint, " +
            "task_name varchar(128) )";
    public static final String DDL_DROP_YDB_TABLE_BUBLIK_OUTBOX =
            "drop table bublik_outbox;";
    public static final String DDL_CREATE_YDB_TABLE_BUBLIK_OUTBOX =
            "create table if not exists bublik_outbox (" +
                    "chunk_id Uint32, " +
                    "start_rowid String, " +
                    "end_rowid String, " +
                    "start_page Uint64, " +
                    "end_page Uint64, " +
                    "schema_name String, " +
                    "table_name String, " +
                    "rows Uint64, " +
                    "task_name String," +
                    "primary key(chunk_id))";
    public static final String DML_INSERT_BUBLIK_OUTBOX_ROWID =
            "insert into bublik_outbox (chunk_id, start_rowid, end_rowid, rows, task_name, schema_name, table_name) " +
                    "values (?, ?, ?, ?, ?, ?, ?)";
    public static final String DML_INSERT_BUBLIK_OUTBOX_CTID =
            "insert into bublik_outbox (chunk_id, start_page, end_page, rows, task_name, schema_name, table_name) " +
                    "values (?, ?, ?, ?, ?, ?, ?)";
    public static final String DML_INSERT_CTID_CHUNKS =
            "insert into public.ctid_chunks (parent_id, start_page, end_page, xidmin, xidmax, task_name, schema_name, table_name, config, status, copied) " +
            "values (?, ?, ?, ?, ?, ?, ?, ?, to_json(?::json), ?, ?)";
    public static final String SQL_CHUNKS_AVG_SYNC =
            // тут можно переделать на max(end_page - start_page) as pages_in_chunk
//            "select schema_name, table_name, task_name, config, AVG(end_page - start_page) as pages_in_chunk, " +
            "select schema_name, table_name, task_name, config, MIN(end_page) - MIN(start_page) as pages_in_chunk, " +
                    "MAX(end_page) max_ctid_end_page, " +
                    "MAX(chunk_id) last_id, " +
//                    "0 as max_xid_min, " +
//                    "MAX(xidmin) max_xid_min, " +
                    "pg_relation_size( schema_name ||'.'|| table_name ) / 8192 as heap_blks_total" +
                    " from public.ctid_chunks o where status = ANY (?) and xidmin is not null group by schema_name, table_name, task_name, config";
    public static final String SQL_CHUNKS_SYNC =
            "select chunk_id, parent_id, start_page, end_page, xidmin, xidmax, schema_name, table_name, config " +
                    " from public.ctid_chunks where status = ANY (?) and xidmin is not null";
    public static final String SQL_CHUNKS_SYNC_WITHOUT_XIDMIN =
            "select chunk_id, parent_id, last_id, start_page, end_page, xidmin, xidmax, schema_name, table_name, config " +
                    " from public.ctid_chunks where status = ANY (?) and xidmin is null";
    public static final String DML_BATCH_INSERT_CTID_CHUNKS =
            "insert into public.ctid_chunks (start_page, end_page, copied, task_name, " +
                    "schema_name, table_name, status, config, required, last_id ) " +
                    "(select * from (select n start_page, case when (n + ? < ?) then (n + ?) else ? end as end_page, ? as copied, ? task_name, " +
                    "? schema_name, ? table_name, ? status, to_json(?::json) config, ? required, ? + row_number() over() - 1 as last_id from generate_series(?, ?, ?) as n) c where start_page <> end_page)";
    public static final String SQL_SELECT_CTID_CHUNKS =
            "select chunk_id, start_page, end_page, schema_name, table_name from public.ctid_chunks where status = 'UNASSIGNED'";
    public static final String SQL_SELECT_MAX_XMIN_XMAX_OF_CHUNK =
//            "select max(xmin::text::int8) xidmin, max(xmax::text::int8) xidmax from $schemaName.$tableName " +
//                    "where ctid >= concat('(', ? ,',1)')::tid and ctid < concat('(', ?,',1)')::tid";
            "select xmin as xidmin, 0 as xidmax from $schemaName.$tableName where ctid >= concat('(', ? ,',1)')::tid and ctid < concat('(', ?,',1)')::tid and " +
                    "age(xmin) = " +
                    "(select min(age(xmin)) from $schemaName.$tableName where ctid >= concat('(', ? ,',1)')::tid and ctid < concat('(', ?,',1)')::tid " +
                    "and age(xmin) > 0)";
    public static final String DML_UPDATE_XID_OF_CTID_CHUNKS =
            "update public.ctid_chunks set xidmin = ?, xidmax = ? where chunk_id = ?";
    public static final String DML_UPDATE_XID_OF_CTID_CHUNKS_BY_LAST_ID =
            "update public.ctid_chunks o set xidmin = (select xidmin from public.ctid_chunks i where i.chunk_id = o.last_id) where chunk_id = ?";
    public static final String PLSQL_DROP_TASK = "CALL DBMS_PARALLEL_EXECUTE.DROP_TASK(task_name => ?)";
    public static final String PLSQL_CREATE_TASK = "CALL DBMS_PARALLEL_EXECUTE.CREATE_TASK(task_name => ?)";
    public static final String PLSQL_CREATE_CHUNK =
            "BEGIN DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID(task_name => ?, table_owner => ?, table_name  => ?, by_row => TRUE, chunk_size  => ?); END;";
    public static final String PLSQL_UPDATE_STATUS_ROWID_CHUNKS = "CALL DBMS_PARALLEL_EXECUTE.SET_CHUNK_STATUS(?,?,?)";
    public static final String PLSQL_UPDATE_STATUS_ROWID_CHUNKS_WITH_ERRORS =
            "CALL DBMS_PARALLEL_EXECUTE.SET_CHUNK_STATUS(task_name => ?,chunk_id => ?,status => ?,err_msg => ?)";
    public static final String DML_UPDATE_CONFIG_CTID_CHUNKS =
            "update public.ctid_chunks set config = to_json(?::json) where chunk_id = ?";
    public static final String DML_UPDATE_COPIED_CTID_CHUNKS =
            "update public.ctid_chunks set copied = ? where chunk_id = ?";
    public static final String DML_UPDATE_UPSERTED_CTID_CHUNKS =
            "update public.ctid_chunks set upserted = ?, end_ts = (case when ? = 0 then null else now() end) where chunk_id = ?";
    public static final String DML_UPDATE_STATUS_CTID_CHUNKS =
            "update public.ctid_chunks set status = ?, err_msg = null where chunk_id = ? and task_name = ?";
    public static final String DML_UPDATE_STATUS_CTID_CHUNKS_WITH_ERRORS =
            "update public.ctid_chunks set status = ?, err_msg = ? where chunk_id = ? and task_name = ?";

    public static final String SQL_PG_INDEX_DEFINITION =
            "select indexdef from pg_indexes where schemaname = ? and tablename = ? and indexname = ?";
    public static final String SQL_PG_INDEX_BASIC_COLUMNS =
            "select ix.indexrelid as id, i.relname, ix.indisunique as uniq, ix.indisprimary as pri, " +
            "case ix.indoption[array_position(ix.indkey, a.attnum)] " +
            "when 0 then '' when 1 then 'desc nulls last' when 2 then 'nulls first' when 3 then 'desc nulls first' end as ascdesc, " +
            "ix.indkey, array_position(ix.indkey, a.attnum)+1 as pos, ix.indnkeyatts, ix.indnatts, a.attname as name, a.attnum as num, " +
            "pg_get_expr(ix.indpred, t.relname::regclass) as filter, " +
            "pg_get_indexdef(i.relname::regclass) as indexdef " +
            "from pg_class t, pg_index ix, pg_class i, pg_attribute a, pg_namespace n " +
            "where n.oid = t.relnamespace and n.nspname = ? and t.relname = ? and ix.indisunique = ? " +
            "and t.oid = ix.indrelid and ix.indexrelid = i.oid and t.oid = a.attrelid and t.relkind = 'r' " +
            "and not exists (select oid from pg_constraint c where c.conindid = ix.indexrelid) " +
            "and a.attnum = any(ix.indkey[0:ix.indnkeyatts-1]) and ix.indisprimary = false";
    public static final String SQL_PG_INDEX_INCLUDE_COLUMNS =
            "select ix.indexrelid as id, i.relname, ix.indisunique as uniq, ix.indisprimary as pri, " +
            "case ix.indoption[array_position(ix.indkey, a.attnum)] " +
            "when 0 then '' when 1 then 'desc nulls last' when 2 then 'nulls first' when 3 then 'desc nulls first' end as ascdesc, " +
            "ix.indkey, array_position(ix.indkey, a.attnum)+1 as pos, ix.indnkeyatts, ix.indnatts, a.attname as name, a.attnum as num, " +
            "pg_get_expr(ix.indpred, t.relname::regclass) as filter, " +
            "pg_get_indexdef(i.relname::regclass) as indexdef " +
            "from pg_class t, pg_index ix, pg_class i, pg_attribute a, pg_namespace n " +
            "where n.oid = t.relnamespace and n.nspname = ? and t.relname = ? and ix.indisunique = ? " +
            "and t.oid = ix.indrelid and ix.indexrelid = i.oid and t.oid = a.attrelid and t.relkind = 'r' " +
            "and not exists (select oid from pg_constraint c where c.conindid = ix.indexrelid) " +
            "and a.attnum = any(ix.indkey[ix.indnkeyatts:]) and ix.indisprimary = false";
    public static String SQL_PG_UNIQUE_CONSTRAINTS =
            "select i.indnullsnotdistinct, c.conname, a.attname, array_position(i.indkey, a.attnum)+1 as pos " +
            "from pg_namespace n, pg_constraint c, pg_index i, pg_class t, pg_attribute a " +
            "where n.oid = t.relnamespace and t.oid = c.conrelid and i.indexrelid = c.conindid and " +
            "i.indisprimary = false and t.oid = a.attrelid and t.relkind = 'r' and a.attnum = any(i.indkey) and " +
            "c.contype = 'u' and n.nspname = ? and t.relname = ?";
    public static final String SQL_PG_TABLE_OPTIONS =
            "select c.oid::int4 as oid, c.reloptions from pg_class c, pg_namespace n " +
            "where n.oid = c.relnamespace and n.nspname = ? and c.relname = ?";

    public static final String DDL_PG_CREATE_TABLE =
            "create table if not exists $schemaName.$tableName ($columnDefinition) ";
    public static final String DDL_PG_CREATE_TABLE_OPTION_CLAUSE =
            "with ($tableOption) ";

    public static String getTableName(String schemaName, String tableName) {
        return schemaName + "." + tableName;
        }
}
