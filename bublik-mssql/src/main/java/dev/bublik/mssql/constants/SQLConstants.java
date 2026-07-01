package dev.bublik.mssql.constants;

public abstract class SQLConstants {
    public static final String DDL_CREATE_SCHEMA =
        "create schema $schemaName";
    public static final String DDL_DROP_SCHEMA =
        "drop schema if exists $schemaName";
    public static final String DDL_CREATE_CHUNK_SEQ =
        "create sequence $schemaName.chunk_seq as int start with 1 increment by 1";
    public static final String DDL_DROP_CHUNK_SEQ =
            "drop sequence $schemaName.chunk_seq";
    public static final String DDL_CREATE_CHUNK_TABLE = """
            create table $schemaName.[$tableName] (
                chunk_id int primary key,
                uuid varchar(36),
                start_page bigint,
                end_page bigint,
                ext_schema varchar(128),
                ext_table varchar(256),
                schema_name varchar(128),
                table_name varchar(256),
                required bigint,
                copied bigint,
                task_name varchar(128),
                status varchar(20)  default 'UNASSIGNED',
                start_ts datetime2,
                end_ts datetime2,
                err_msg varchar(2048))
            """;
    public static final String DDL_CREATE_CHUNK_EXT_TABLE = """
        \n create table $schemaName.[_ext_$extTableName] (
            chunk_id int,
            page int,
            $columns
            )""";
    public static final String SQL_VALUES_FROM_EXT_TABLE =
            "select $columns from $schemaName.[_ext_$extTableName] where chunk_id = ?";
    public static final String DDL_DROP_CHUNK_TABLE =
        "drop table if exists $schemaName.[$tableName]";
    public static final String SQL_CLUSTERING_KEY =
        "SELECT ic.key_ordinal, c.name as column_name, t.name as column_type, ic.is_descending_key, c.max_length, c.is_nullable " +
            "FROM sys.indexes i " +
            "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id " +
            "JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id " +
            "JOIN sys.types t ON c.user_type_id = t.user_type_id " +
            "WHERE i.object_id = OBJECT_ID(?) " +
            "AND i.type = 1 ";
    public static final String DML_INSERT_CHUNKS = """
            \n insert into $schemaName.[$tableName] (chunk_id, ext_schema, ext_table, schema_name, table_name, required, task_name)
                (select chunk_id, ?, ?, ?, ?, ?, ? from $schemaName.[_ext_$extTableName])
            """;
    public static final String DML_INSERT_EXT_CHUNKS = """
            WITH ChunkPoints AS (
                SELECT $columns , RowNum, ROW_NUMBER() OVER (ORDER BY RowNum ASC) AS rNum
                FROM (
                    SELECT $columns , ROW_NUMBER() OVER (ORDER BY $colsAscDesc) AS RowNum
                    FROM $tableName
                ) AS t
                WHERE RowNum % ? = 0 OR RowNum = 1
            )
            INSERT INTO $schemaName.[_ext_$extTableName] (chunk_id, page, $fromToColumns)
            SELECT NEXT VALUE FOR $schemaName.chunk_seq, rNum as page, $leadColumns FROM ChunkPoints
            """;
    public static final String DML_UPDATE_STATUS_CHUNK_TABLE =
            "update $tableName set status = ?, err_msg = null where chunk_id = ?";
    public static final String DML_UPDATE_STATUS_CHUNK_TABLE_WITH_ERRORS =
            "update $tableName set status = ?, err_msg = ? where chunk_id = ?";
    public static final String DML_UPDATE_UUID_COPIED_CHUNK_TABLE =
            "update $tableName set copied = ? where chunk_id = ?";
}
