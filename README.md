
# Tool for Data Transfer from Oracle to PostgreSQL OR from PostgreSQL to PostgreSQL

This tool facilitates the efficient transfer of data from Oracle to PostgreSQL.<br>
The quickest method for extracting data from Oracle is by using `ROWID` (employing `dbms_parallel_execute` to segment the data into chunks). 
In case of PostgreSQL, we should split a table into chunks by CTID.<br>
Conversely, the fastest way to input data into PostgreSQL is through the `COPY` command in binary format.

## Supported Types
| ORACLE                   | Postgresql (possible types)                          |
|:-------------------------|:-----------------------------------------------------|
| char, varchar, varchar2  | char, bpchar, varchar, text                          |
| CLOB                     | varchar, text                                        |
| BLOB                     | bytea                                                |
| date                     | date, timestamp, timestamptz                         |
| timestamp                | timestamp, timestamptz                               |
| timestamp with time zone | timestamptz                                          |
| number                   | numeric, smallint, bigint, integer, double precision |

## The Task
The objective is to migrate two tables (TABLE1 and TABLE2) from an Oracle schema (ORASCHEMA) to a PostgreSQL database.



### Step 1

<ul><li>Prepare Oracle environment</li></ul>

```
git clone https://github.com/dimarudik/bublik.git
cd bublik
```

```
./dockerfiles/buildContainerImage.sh -x -v 21.3.0
```

```
docker run --name oracle \
    -p 1521:1521 -p 5500:5500 \
    -e ORACLE_PWD=oracle_4U \
    -v ./dockerfiles/scripts:/docker-entrypoint-initdb.d \
    -d oracle/database:21.3.0-xe
```

<ul><li>Create the source tables</li></ul>

```
sqlplus test/test@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=xepdb1)))
```

<ul><li>Build the jar file</li></ul>

```shell
mvn clean package -DskipTests
cd target
mkdir logs
```

<ul><li>Create empty tables in the Postgresql database</li></ul>
<ul><li>Prepare the connection parameters file props.yaml</li></ul>

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


<ul><li>Prepare the table parameters file rules.json</li></ul>

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
>  **WARNING**: `excludedSourceColumns` and `excludedTargetColumns` are case sensetive. Usually for quoted values use upper case for Oracle (e.g. "COLUMN_VANE") and lower case for Postgres (e.g. "column_name"). In other cases use upper case.


```
docker run --name postgres \
        -e POSTGRES_USER=postgres \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=postgres \
        -p 5432:5432 \
        -v ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql \
        -v ./sql/.psqlrc:/var/lib/postgresql/.psqlrc \
        -d postgres \
        -c shared_preload_libraries="pg_stat_statements,auto_explain" \
        -c max_connections=200 \
        -c logging_collector=on \
        -c log_directory=pg_log \
        -c log_filename=%u_%a.log \
        -c log_min_duration_statement=3 \
        -c log_statement=all \
        -c auto_explain.log_min_duration=0 \
        -c auto_explain.log_analyze=true
```

```
psql postgresql://test:test@localhost/postgres
```

### Step 2
Halt any changes to the movable tables in the source database (Oracle)<br>
Prepare data chunks in Oracle using the same user credentials specified in `ora2pgsql` tool (`fromProperties` in `props.yaml`):

```
exec DBMS_PARALLEL_EXECUTE.drop_task(task_name => 'TABLE1_TASK');
exec DBMS_PARALLEL_EXECUTE.create_task (task_name => 'TABLE1_TASK');
exec DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid (task_name   => 'TABLE1_TASK', table_owner => 'ORAOWNER', table_name  => 'TABLE1', by_row => TRUE, chunk_size  => 100000);

exec DBMS_PARALLEL_EXECUTE.drop_task(task_name => 'TABLE2_TASK');
exec DBMS_PARALLEL_EXECUTE.create_task (task_name => 'TABLE2_TASK');
exec DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid (task_name   => 'TABLE2_TASK', table_owner => 'ORAOWNER', table_name  => 'TABLE2', by_row => TRUE, chunk_size  => 100000);
```

### Step 3
Run the tool
```
java -jar bublik-1.2.jar props.yaml rules.json 
```

<ul><li>To prevent heap pressure, use `-Xmx16g`</li></ul>
<ul><li>Monitor the logs at `log/app.log`.</li></ul>
<ul><li>Track progress in Oracle:</li></ul>

```
select status, count(*), round(100 / sum(count(*)) over() * count(*),2) pct 
    from dba_parallel_execute_chunks where task_owner = 'ORAOWNER' group by  status;
```
