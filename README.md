
# Is a tool to copy data from Oracle to Postgresql

The fastest way to obtain data from Oracle is to get it by `ROWID` (use `dbms_parallel_execute` to prepare chunks)
The fastest way to put data to Postgresql is to insert it by using `COPY` in binary format (except LOB's)

Run
```
java -jar -Xmx4g ora2pgsql-1.1-SNAPSHOT.jar props.yaml rules.json 
```
## The Task
We need to transfer two tables (TABLE1, TABLE2) of ORASCHEMA from Oracle db to Postgresql db

### Step 0
Build the jar file
```shell
git clone https://github.com/dimarudik/ora2pgsql.git
cd ora2pgsql
mvn clean package -DskipTests
cd target
mkdir logs
```
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
    "fromSchemaName" : "ORASCHEMA", 
    "fromTableName" : "TABLE1", 
    "toSchemaName" : "PGSCHEMA", 
    "toTableName" : "TABLE1", 
    "fetchHintClause" : "/*+ parallel(2) */", 
    "fetchWhereClause" : "1 = 1", 
    "fromTaskName" : "TABLE1_TASK"
  }
]
```

### Step 1
You have to stop changes at the source db (Oracle) 

### Step 2
Prepare chunks of tables in Oracle (do it by the same user as you are going to connect to Oracle via `ora2pgsql` tool, see `fromProperties` in `props.yaml`)
```
exec DBMS_PARALLEL_EXECUTE.drop_task(task_name => 'TABLE1_TASK');
exec DBMS_PARALLEL_EXECUTE.create_task (task_name => 'TABLE1_TASK');
exec DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid (task_name   => 'TABLE1_TASK', table_owner => 'ORAOWNER', table_name  => 'TABLE1', by_row => TRUE, chunk_size  => 100000);
exec DBMS_PARALLEL_EXECUTE.drop_task(task_name => 'TABLE2_TASK');
exec DBMS_PARALLEL_EXECUTE.create_task (task_name => 'TABLE2_TASK');
exec DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid (task_name   => 'TABLE2_TASK', table_owner => 'ORAOWNER', table_name  => 'TABLE2', by_row => TRUE, chunk_size  => 100000);
```
