
# Is a tool to copy data from Oracle to Postgresql

The fastest way to obtain data from Oracle is to get it by `ROWID` (use `dbms_parallel_execute` to prepare chunks)
The fastest way to put data to Postgresql is to insert it by using `COPY` in binary format (except LOB's)

Build
```
mvn clean package -DskipTests
```

Run
```
java -jar -Xmx4g ora2pgsql-1.1-SNAPSHOT.jar props.yaml rules.json 
```
## The Task
We need to transfer two tables from Oracle to Postgresql

### Step 0

Prepare files of parameters

props.yaml
```yaml
threadCount: 20

fromProperties:
  url: jdbc:oracle:thin:@(description=(address=(host=oracle-host)(protocol=tcp)(port=1521))(connect_data=(service_name=ORA)))
  user: oraowner
  password: oraowner
toProperties:
  url: jdbc:postgresql://postgres-host:5432/db
  user: pgowner
  password: pgowner
```

rules.json
```json
[
  { 
    "fromSchemaName" : "SourceOwner", 
    "fromTableName" : "SourceTableName", 
    "toSchemaName" : "TargetOwner", 
    "toTableName" : "TargetTableName", 
    "fetchHintClause" : "/*+ parallel(4) */", 
    "fetchWhereClause" : "1 = 1 and created_at > sysdate - 30", 
    "fromTaskName" : "OracleTaskName",
    "excludedSourceColumns" : [ "Column1", "Column2" ],
    "excludedTargetColumns" : [ "Column3" ]
  }
]
```

### Step 1

You have to stop changes at the source db (Oracle) 

### Step 2

Prepare chunks of tables in Oracle (do it by the same user as you are going to connect to Oracle via `ora2pgsql` tool)

```
exec DBMS_PARALLEL_EXECUTE.drop_task(task_name => 'TABLE1_TASK');
exec DBMS_PARALLEL_EXECUTE.create_task (task_name => 'TABLE1_TASK');
exec DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid (task_name   => 'TABLE1_TASK', table_owner => 'ORAOWNER', table_name  => 'TABLE1', by_row => TRUE, chunk_size  => 100000);
exec DBMS_PARALLEL_EXECUTE.drop_task(task_name => 'TABLE2_TASK');
exec DBMS_PARALLEL_EXECUTE.create_task (task_name => 'TABLE2_TASK');
exec DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid (task_name   => 'TABLE2_TASK', table_owner => 'ORAOWNER', table_name  => 'TABLE2', by_row => TRUE, chunk_size  => 100000);
```
