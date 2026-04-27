package dev.bublik.mssql.constants;

public abstract class SQLConstants {
    public static final String DDL_CREATE_SCHEMA =
        "create schema bublik";
    public static final String DDL_CREATE_SEQUENCE =
        "create sequence bublik.[_seq] start with 1 increment by 1 minvalue 1 maxvalue 99999999";
    public static final String DDL_CREATE_CHUNK_TABLE =
        "create table bublik.[$tableName] (" +
            "chunk_id int identity(1,1) primary key, " +
            "uuid varchar(36), " +
            "start_page bigint, " +
            "end_page bigint, " +
            "schema_name varchar(128), " +
            "table_name varchar(256), " +
            "config json, " +
            "required bigint, " +
            "copied bigint, " +
            "task_name varchar(128), " +
            "status varchar(20)  default 'UNASSIGNED', " +
            "start_ts datetime2, " +
            "end_ts datetime2, " +
            "err_msg varchar(2048), " +
            "constraint b_uq unique (uuid, start_page, end_page, task_name, status) )";
    public static final String DDL_CREATE_CHUNK_EXT_TABLE =
        "create table bublik.[_ext_$tableName] (" +
            "page bigint identity(1,1) primary key, " +
            "chunk_id int " +
            "$columns " +
            ")";
    public static final String DDL_DROP_CHUNK_TABLE =
        "drop table if exists bublik.[$tableName]";
    public static final String SQL_CLUSTERING_KEY =
        "SELECT ic.key_ordinal, c.name as column_name, t.name as column_type, ic.is_descending_key, c.max_length, c.is_nullable " +
            "FROM sys.indexes i " +
            "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id " +
            "JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id " +
            "JOIN sys.types t ON c.user_type_id = t.user_type_id " +
            "WHERE i.object_id = OBJECT_ID(?) " +
            "AND i.type = 1 ";
    public static final String SQL_CHUNKS = """
            WITH ChunkPoints AS (
                SELECT $columns , RowNum
                FROM (
                    SELECT $columns , ROW_NUMBER() OVER (ORDER BY $columns) AS RowNum
                    FROM $tableName
                ) AS t
                WHERE RowNum % 50000 = 0 OR RowNum = 1
            )
            SELECT
                uuid AS start_uuid,
                id AS start_id,
                LEAD(uuid) OVER (ORDER BY RowNum) AS end_uuid,
                LEAD(id) OVER (ORDER BY RowNum) AS end_id
            FROM ChunkPoints""";
}
