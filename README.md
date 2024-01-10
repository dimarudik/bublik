
# Is a tool to copy data from Oracle to Postgresql

The fastest way to obtain data from Oracle is to get it by `ROWID` (use `dbms_parallel_execute` to prepare chunks)
The fastest way to put data to Postgresql is to insert it by using `COPY` in binary format (except LOB's)

## The Task
We need to transfer two tables (TABLE1, TABLE2) of ORASCHEMA from Oracle to Postgresql

### Step 1
<ul><li>create empty tables in Postgresql db</li></ul>
<ul><li>build the jar file</li></ul>

```shell
git clone https://github.com/dimarudik/ora2pgsql.git
cd ora2pgsql
mvn clean package -DskipTests
cd target
mkdir logs
```
<ul><li>prepare file of connection parameters props.yaml</li></ul>

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
<ul><li>prepare file of table parameters rules.json</li></ul>

```json
[
  { 
    "fromSchemaName" : "ORASCHEMA", 
    "fromTableName" : "TABLE1", 
    "toSchemaName" : "PGOWNER", 
    "toTableName" : "TABLE1", 
    "fetchHintClause" : "/*+ parallel(2) */", 
    "fetchWhereClause" : "1 = 1", 
    "fromTaskName" : "TABLE1_TASK"
  },
  {
    "fromSchemaName" : "ORASCHEMA",
    "fromTableName" : "TABLE2",
    "toSchemaName" : "PGOWNER",
    "toTableName" : "TABLE2",
    "fetchHintClause" : "/*+ parallel(2) */",
    "fetchWhereClause" : "1 = 1",
    "fromTaskName" : "TABLE2_TASK"
  }
]
```

### Step 2
Stop changes of movable tables at the source db (Oracle)<br>
Prepare chunks of tables in Oracle<br> 
Do it by the same user as you are going to connect to Oracle via `ora2pgsql` tool<br>
(see `fromProperties` in `props.yaml`)

```
exec DBMS_PARALLEL_EXECUTE.drop_task(task_name => 'TABLE1_TASK');
exec DBMS_PARALLEL_EXECUTE.create_task (task_name => 'TABLE1_TASK');
exec DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid (task_name   => 'TABLE1_TASK', table_owner => 'ORAOWNER', table_name  => 'TABLE1', by_row => TRUE, chunk_size  => 100000);

exec DBMS_PARALLEL_EXECUTE.drop_task(task_name => 'TABLE2_TASK');
exec DBMS_PARALLEL_EXECUTE.create_task (task_name => 'TABLE2_TASK');
exec DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid (task_name   => 'TABLE2_TASK', table_owner => 'ORAOWNER', table_name  => 'TABLE2', by_row => TRUE, chunk_size  => 100000);
```

### Step 3
Run
```
java -jar ora2pgsql-1.1-SNAPSHOT.jar props.yaml rules.json 
```

<ul><li>to avoid heap pressure please use -Xmx16g</li></ul>
<ul><li>see log/app.log</li></ul>
<ul><li>progress in Oracle:</li></ul>

```
select status, count(*), round(100 / sum(count(*)) over() * count(*),2) pct 
    from dba_parallel_execute_chunks where task_owner = 'ORAOWNER' group by  status;
```
