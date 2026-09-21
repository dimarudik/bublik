package dev.bublik.oracle.constants;

public abstract class SQLConstants {
    public static final String PLSQL_DROP_TASK = "CALL DBMS_PARALLEL_EXECUTE.DROP_TASK(task_name => ?)";
    public static final String PLSQL_CREATE_TASK = "CALL DBMS_PARALLEL_EXECUTE.CREATE_TASK(task_name => ?)";
    public static final String PLSQL_CREATE_CHUNK =
            "BEGIN DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID(task_name => ?, table_owner => ?, table_name  => ?, by_row => TRUE, chunk_size  => ?); END;";
    public static final String PLSQL_UPDATE_STATUS_ROWID_CHUNKS = "CALL DBMS_PARALLEL_EXECUTE.SET_CHUNK_STATUS(?,?,?)";
    public static final String PLSQL_UPDATE_STATUS_ROWID_CHUNKS_WITH_ERRORS =
            "CALL DBMS_PARALLEL_EXECUTE.SET_CHUNK_STATUS(task_name => ?,chunk_id => ?,status => ?,err_msg => ?)";
    public static final String DML_UPDATE_STATUS_CHUNK_TABLE_ASSIGNED =
            "update $tableName set status = ?, err_msg = null, start_ts = CURRENT_TIMESTAMP where chunk_id = ?";
    public static final String DML_UPDATE_STATUS_CHUNK_TABLE_PROCESSED =
            "update $tableName set status = ?, err_msg = null, end_ts = CURRENT_TIMESTAMP where chunk_id = ?";
    public static final String DML_UPDATE_STATUS_CHUNK_TABLE_WITH_ERRORS =
            "update $tableName set status = ?, err_msg = ? where chunk_id = ?";
    public static final String DDL_CREATE_CHUNK_TABLE =
            "create table $tableName (" +
            "        chunk_id number generated always as identity (start with -1 increment by -1) primary key, " +
            "        uuid varchar2(36), " +
            "        start_rowid varchar2(18), " +
            "        end_rowid varchar2(18), " +
            "        schema_name varchar2(128), " +
            "        table_name varchar2(256), " +
            "        config clob, " +
            "        required number(19), " +
            "        copied number(19), " +
            "        task_name varchar2(128), " +
            "        status varchar2(20) default 'UNASSIGNED', " +
            "        start_ts timestamp, " +
            "        end_ts timestamp, " +
            "        err_msg varchar2(2048), " +
            "        constraint uq_bublik unique (uuid, start_rowid, end_rowid, task_name, status) )";
    public static final String DDL_DROP_CHUNK_TABLE = "drop table $tableName";
    public static final String PLSQL_FULFILL_PART_CHUNKS = """
        DECLARE
          l_batch_size NUMBER := :1;
          l_schema     VARCHAR2(128) := :2;
          l_table      VARCHAR2(256) := :3;
          l_partition  VARCHAR2(256) := :4;
          l_task_name  VARCHAR2(128) := :5;
          l_segment_type  VARCHAR2(128) := :6;
        
          c_source       SYS_REFCURSOR;
          l_sql          CLOB;
          l_create       CLOB;
          l_chunks_sql   CLOB;
          l_drop         CLOB;
          l_insert       CLOB;
        
          TYPE t_rowids IS TABLE OF ROWID INDEX BY PLS_INTEGER;
          l_rowids t_rowids;
        
          l_start_id ROWID;
          l_end_id   ROWID;
        BEGIN
          l_sql := 'SELECT rowid FROM '
                   || l_schema || '.' || l_table || ' ' || l_segment_type || ' (' || l_partition || ') p order by p.rowid';
        
          l_create := 'create global temporary table temp_' || l_task_name || ' (start_id rowid, end_id rowid) on commit preserve rows';
          execute immediate l_create;
        
          l_insert := 'INSERT INTO temp_' || l_task_name || ' (start_id, end_id) VALUES (:1, :2)';
        
          OPEN c_source FOR l_sql;
        
          LOOP
            FETCH c_source BULK COLLECT INTO l_rowids LIMIT l_batch_size;
            EXIT WHEN l_rowids.COUNT = 0;
        
            l_start_id := l_rowids(1);
            l_end_id   := l_rowids(l_rowids.COUNT);
        
            execute immediate l_insert USING l_start_id, l_end_id;
        
          l_chunks_sql := l_chunks_sql
                    || 'SELECT CHARTOROWID(''' || ROWIDTOCHAR(l_start_id) || ''') start_id, '
                    || 'CHARTOROWID(''' || ROWIDTOCHAR(l_end_id) || ''') end_id FROM dual';
        
          END LOOP;
          CLOSE c_source;
  
          commit;
        
          l_chunks_sql := 'SELECT start_id, end_id FROM temp_' || l_task_name;
        
          DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_SQL(
            l_task_name,
            l_chunks_sql,
            TRUE
          );
          
          execute immediate 'truncate table temp_' || l_task_name;
        
          l_drop := 'drop table temp_' || l_task_name;
          execute immediate l_drop;
        
        END;
        """;
}
