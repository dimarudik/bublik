package dev.bublik.postgres.constants;

public abstract class SQLConstants {
    public static final String DDL_CREATE_CHUNK_TABLE =
            "create table $tableName (" +
                    "chunk_id int generated always as identity primary key, " +
                    "uuid varchar(36), " +
                    "start_page bigint, " +
                    "end_page bigint, " +
                    "schema_name varchar(128), " +
                    "table_name varchar(256), " +
                    "config jsonb, " +
                    "required bigint, " +
                    "copied bigint, " +
                    "task_name varchar(128), " +
                    "status varchar(20)  default 'UNASSIGNED', " +
                    "start_ts timestamp, " +
                    "end_ts timestamp, " +
                    "err_msg varchar(2048), " +
                    "unique (uuid, start_page, end_page, task_name, status) )";
    public static final String DDL_DROP_CHUNK_TABLE =
            "drop table $tableName";
    public static final String DDL_TRUNCATE_CHUNK_TABLE =
            "truncate table $tableName";
    public static final String SQL_NUMBER_OF_TUPLES =
            "select reltuples, relpages, relkind from pg_class " +
                    "where relnamespace::regnamespace::text = ? and relname = ?";
    public static final String DML_BATCH_INSERT_CHUNK_TABLE =
            "insert into $tableName (start_page, end_page, copied, task_name, " +
                    "schema_name, table_name, status, config, required ) " +
                    "(select * from (select n start_page, n + ? as end_page, ? as copied, ? task_name, " +
                    "? schema_name, ? table_name, ? status, to_json(?::json) config, ? required from generate_series(?, ?, ?) as n) c where start_page <> end_page)";
    public static final String SQL_MAX_END_PAGE =
            "select max(end_page) as max_end_page from $tableName where task_name = ?";
    public static final String DML_UPDATE_STATUS_CHUNK_TABLE_ASSIGNED =
            "update $tableName set status = ?, err_msg = null, start_ts = now() where chunk_id = ?";
    public static final String DML_UPDATE_STATUS_CHUNK_TABLE_PROCESSED =
            "update $tableName set status = ?, err_msg = null, end_ts = now() where chunk_id = ?";
    public static final String DML_UPDATE_STATUS_CHUNK_TABLE_WITH_ERRORS =
            "update $tableName set status = ?, err_msg = ? where chunk_id = ?";
    public static final String DML_UPDATE_UUID_COPIED_CHUNK_TABLE =
            "update $tableName set copied = ? where chunk_id = ?";
    public static final String SQL_HEAP_BLKS_TOTAL =
            "select pg_relation_size( ? ) / 8192 as heap_blks_total";

    public static final String DDL_CREATE_OUTBOX_TABLE =
            "create table $tableName (" +
                    "chunk_id int primary key, " +
                    "task_name varchar(128), " +
                    "copied bigint)";
    public static final String DDL_DROP_OUTBOX_TABLE =
            "drop table $tableName";
    public static final String DML_INSERT_OUTBOX_TABLE =
            "insert into $tableName (chunk_id, task_name, copied) " +
                    "values (?, ?, ?)";
    public static final String DML_SELECT_OUTBOX_TABLE =
            "select chunk_id, task_name, copied from $tableName where chunk_id = ?";

/*
    public static final String DML_INSERT_OUTBOX_TABLE =
            "insert into bublik_outbox (chunk_id, start_page, end_page, rows, task_name, schema_name, table_name, uuid) " +
                    "values (?, ?, ?, ?, ?, ?, ?, ?)";
*/

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
    public static final String DDL_CREATE_TABLE =
            "create table if not exists $schemaName.$tableName ($columnDefinition) ";
    public static final String SQL_PG_CURRENT_LSN_AND_XID =
            "select pg_current_wal_lsn(), pg_current_xact_id()";
/*
    public static final String DML_UPDATE_CONFIG_CTID_CHUNKS =
            "update public.ctid_chunks set config = to_json(?::json) where chunk_id = ?";
    public static final String SQL_MIN_MAX_BLOCK_NUMBER =
            "select " +
                    "(min(ctid)::text::point)[0]::bigint AS block_min, " +
                    "(max(ctid)::text::point)[0]::bigint + 1 AS block_max " +
                    "from ";
    public static final String DDL_TRUNCATE_PG_TABLE_BUBLIK_OUTBOX =
            "truncate table public.bublik_outbox;";
    public static String SQL_TOTAL_CHUNKS =
            "select count(1) total_chunks from public.ctid_chunks";
    public static final String SQL_PG_INDEX_DEFINITION =
            "select indexdef from pg_indexes where schemaname = ? and tablename = ? and indexname = ?";
    public static final String DML_UPDATE_UPSERTED_CTID_CHUNKS =
            "update public.ctid_chunks set upserted = ?, end_ts = (case when ? = 0 then null else now() end) where chunk_id = ?";
    public static final String DML_INSERT_CTID_CHUNKS =
            "insert into public.ctid_chunks (parent_id, start_page, end_page, xidmin, xidmax, task_name, schema_name, table_name, config, status, copied) " +
            "values (?, ?, ?, ?, ?, ?, ?, ?, to_json(?::json), ?, ?)";
    public static final String SQL_HEAP_BLKS_TOTAL_SYNC =
            "select pg_relation_size( schema_name ||'.'|| table_name ) / 8192 as heap_blks_total " +
            "from public.ctid_chunks o where chunk_id = ?";
    public static final String SQL_CHUNKS_AVG_SYNC =
            "select schema_name, table_name, task_name, config, MIN(end_page) - MIN(start_page) as pages_in_chunk, " +
            "MAX(end_page) max_ctid_end_page, " +
            "MAX(chunk_id) last_id, " +
            "pg_relation_size( schema_name ||'.'|| table_name ) / 8192 as heap_blks_total" +
            " from public.ctid_chunks o where status = ANY (?) and xidmin is not null group by schema_name, table_name, task_name, config";
    public static final String SQL_CHUNKS_SYNC =
            "select chunk_id, parent_id, start_page, end_page, xidmin, xidmax, schema_name, table_name, config, status from " +
            "((select chunk_id, parent_id, start_page, end_page, xidmin, xidmax, schema_name, table_name, config, status " +
            "from public.ctid_chunks where status = ANY (?) and xidmin is not null) " +
            "union all " +
            "(select chunk_id, parent_id, start_page, end_page, xidmin, xidmax, schema_name, table_name, config, status " +
            "from ctid_chunks r where not exists (select p.chunk_id from ctid_chunks p where p.parent_id = r.chunk_id) and r.status = 'SYNCED')) a " +
            "order by xidmin";
    public static final String SQL_CHUNKS_SYNC_WITHOUT_XIDMIN =
            "select chunk_id, parent_id, last_id, start_page, end_page, xidmin, xidmax, schema_name, table_name, config " +
                    " from public.ctid_chunks where status = ANY (?) and xidmin is null";
    public static final String DML_BATCH_INSERT_CTID_CHUNKS =
            "insert into public.ctid_chunks (start_page, end_page, copied, task_name, " +
            "schema_name, table_name, status, config, required, last_id ) " +
            "(select * from (select n start_page, case when (n + ? < ?) then (n + ?) else ? end as end_page, ? as copied, ? task_name, " +
            "? schema_name, ? table_name, ? status, to_json(?::json) config, ? required, ? + row_number() over() - 1 as last_id from generate_series(?, ?, ?) as n) c where start_page <> end_page)";
    public static final String DML_BATCH_INSERT_CTID_CHUNKS_V2 =
            "insert into public.ctid_chunks (start_page, end_page, copied, task_name, " +
            "schema_name, table_name, status, config, required, last_id, xidmin ) " +
            "(select * from (select n start_page, n + ? as end_page, ? as copied, ? task_name, " +
            "? schema_name, ? table_name, ? status, to_json(?::json) config, ? required, ? + row_number() over() - 1 as last_id, ? as xidmin from generate_series(?, ?, ?) as n) c where start_page <> end_page)";
    public static final String DML_UPDATE_XIDMAX_CTID_CHUNKS =
            "update public.ctid_chunks set xidmax = ? where chunk_id = ?";
    public static final String SQL_SELECT_HAS_UNCOMMITED_TRANSACTIONS =
            "select min(xmax::text::int8) uncommitted from $schemaName.$tableName " +
            "where ctid >= concat('(', ? ,',1)')::tid and ctid < concat('(', ?,',1)')::tid " +
            "and age(xmin) > 0 " +
            "and xmax::text::int8 > 0 " +
            "and coalesce(pg_xact_status(xmax::text::xid8),'committed') = 'in progress'";
    public static final String SQL_SELECT_CTID_CHUNKS =
            "select chunk_id, start_page, end_page, schema_name, table_name from public.ctid_chunks where status = 'UNASSIGNED'";
    public static final String SQL_SELECT_MAX_XMIN_XMAX_OF_CHUNK =
            "select max(xmin::text::int8) as xidmin, 0 as xidmax from $schemaName.$tableName where ctid >= concat('(', ? ,',1)')::tid and " +
            "ctid < concat('(', ?,',1)')::tid and " +
            "age(xmin) = " +
            "(select min(age(xmin)) from $schemaName.$tableName where ctid >= concat('(', ? ,',1)')::tid and ctid < concat('(', ?,',1)')::tid " +
            "and age(xmin) > 0)";
    public static final String DML_UPDATE_XID_OF_CTID_CHUNKS =
            "update public.ctid_chunks set xidmin = ?, xidmax = ? where chunk_id = ?";
    public static final String DML_UPDATE_XID_OF_CTID_CHUNKS_BY_LAST_ID =
            "update public.ctid_chunks o set " +
            "xidmin = (select min(xidmin) from public.ctid_chunks i " +
            "where i.chunk_id < o.last_id and i.task_name = o.task_name) " +
            "where chunk_id = ?";
*/
}
