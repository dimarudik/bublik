# Tool for Data Transfer from Oracle to PostgreSQL or from PostgreSQL to PostgreSQL

This tool facilitates the efficient transfer of data from Oracle to PostgreSQL or from PostgreSQL to PostgreSQL.<br>
The quickest method for extracting data from Oracle is by using `ROWID` (employing `dbms_parallel_execute` to segment the data into chunks). 
In case of PostgreSQL, we should split a table into chunks by `CTID`.<br>
As you know, the fastest way to input data into PostgreSQL is through the `COPY` command in binary format.

* [Supported Types](#Supported-Types)
    * [1. Oracle To PostgreSQL](#1-Oracle-To-PostgreSQL)

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

## 1 Oracle To PostgreSQL
The objective is to migrate tables `TABLE1` `Table2` from an Oracle schema `TEST` to a PostgreSQL database.



### Step 1

<ul><li>Prepare Oracle environment</li></ul>

arm64:
```
docker run --name oracle \
    -p 1521:1521 -p 5500:5500 \
    -e ORACLE_PWD=oracle_4U \
    -v ./dockerfiles/scripts:/docker-entrypoint-initdb.d \
    -d dimarudik/oracle_arm64:19.3.0-ee
```

x86_64:
```
docker run --name oracle \
    -p 1521:1521 -p 5500:5500 \
    -e ORACLE_PWD=oracle_4U \
    -v ./dockerfiles/scripts:/docker-entrypoint-initdb.d \
    -d dimarudik/oracle_x86_64:19.3.0-ee
```


> [!IMPORTANT] 
> The source table `TEST.TABLE1` will be created and fulfilled during oracle docker container startup

<ul><li>How to connect</li></ul>

```
sqlplus 'test/test@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=ORCLPDB1)))'
```
> [!TIP] 
> [How to install Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html)

<ul><li>Prepare PostgreSQL environment</li></ul>

```
docker run --name postgres \
        -e POSTGRES_USER=postgres \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=postgres \
        -p 5432:5432 \
        -v ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql \
        -v ./sql/.psqlrc:/var/lib/postgresql/.psqlrc \
        -v ./sql/bublik.png:/var/lib/postgresql/bublik.png \
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

>  **WARNING**: The empty target table `PUBLIC.TABLE1` will be created during postgre docker container startup

<ul><li>How to connect</li></ul>

```
psql postgresql://test:test@localhost/postgres
```

<ul><li>Prepare the connection parameters file ./sql/ora2pg.yaml</li></ul>

```yaml
threadCount: 10

fromProperties:
  url: jdbc:oracle:thin:@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=ORCLPDB1)))
  user: test
  password: test
toProperties:
  url: jdbc:postgresql://localhost:5432/postgres
  user: test
  password: test
```


<ul><li>Prepare the table parameters file ./sql/ora2pg.json</li></ul>

```json
[
  {
    "fromSchemaName" : "TEST",
    "fromTableName" : "TABLE1",
    "toSchemaName" : "PUBLIC",
    "toTableName" : "TABLE1",
    "fetchHintClause" : "/*+ no_index(TABLE1) */",
    "fetchWhereClause" : "1 = 1",
    "fromTaskName" : "TABLE1_TASK",
    "columnToColumn" : {
      "id"          : "id",
      "name"        : "name",
      "create_at"   : "create_at",
      "update_at"   : "update_at",
      "gender"      : "gender",
      "byteablob"   : "byteablob",
      "textclob"    : "textclob"
    }
  },
  {
    "fromSchemaName" : "TEST",
    "fromTableName" : "TABLE2",
    "toSchemaName" : "PUBLIC",
    "toTableName" : "TABLE2",
    "fetchHintClause" : "/*+ no_index(TABLE2) */",
    "fetchWhereClause" : "1 = 1",
    "fromTaskName" : "TABLE2_TASK",
    "columnToColumn" : {
      "id"          : "id",
      "name"        : "name",
      "create_at"   : "create_at",
      "update_at"   : "update_at",
      "gender"      : "gender",
      "byteablob"   : "byteablob",
      "textclob"    : "textclob"
    }
  }
]
```

<ul><li>Build the jar file</li></ul>

```shell
mvn clean package -DskipTests
```

### Step 2
Halt any changes to the movable tables in the source database (Oracle)<br>
Prepare data chunks in Oracle using the same user credentials specified in `bublik` tool (`fromProperties` in `props.yaml`):

```
exec dbms_parallel_execute.drop_task(task_name => 'TABLE1_TASK');
exec dbms_parallel_execute.create_task (task_name => 'TABLE1_TASK');
begin 
    dbms_parallel_execute.create_chunks_by_rowid (  task_name   => 'TABLE1_TASK',
                                                    table_owner => 'TEST',
                                                    table_name  => 'TABLE1',
                                                    by_row => TRUE,
                                                    chunk_size  => 100000 );
end;
/
exec dbms_parallel_execute.drop_task(task_name => 'TABLE2_TASK');
exec dbms_parallel_execute.create_task (task_name => 'TABLE2_TASK');
begin 
    dbms_parallel_execute.create_chunks_by_rowid (  task_name   => 'TABLE2_TASK',
                                                    table_owner => 'TEST',
                                                    table_name  => 'Table2',
                                                    by_row => TRUE,
                                                    chunk_size  => 100000 );
end;
/
```

### Step 3
Run the tool
```
java -jar ./target/bublik-1.2.jar -c ./sql/ora2pg.yaml -m ./sql/ora2pg.json
```

<ul><li>To prevent heap pressure, use `-Xmx16g`</li></ul>
<ul><li>Monitor the logs at `logs/app.log`.</li></ul>
<ul><li>Track progress in Oracle:</li></ul>

```
select status, count(*), round(100 / sum(count(*)) over() * count(*),2) pct 
    from user_parallel_execute_chunks group by status;
```

## 2. PostgreSQL To PostgreSQL
The objective is to migrate table SOURCE to table TARGET from one PostgreSQL database to another.
> [!TIP] 
> To simplify test case we're using same database

### Step 1
<ul><li>Prepare PostgreSQL environment</li></ul>

```
docker run --name postgres \
        -e POSTGRES_USER=postgres \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=postgres \
        -p 5432:5432 \
        -v ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql \
        -v ./sql/.psqlrc:/var/lib/postgresql/.psqlrc \
        -v ./sql/bublik.png:/var/lib/postgresql/bublik.png \
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

>  **WARNING**: SOURCE & TARGET tables will be created during postgre docker container startup

<ul><li>How to connect</li></ul>

```
psql postgresql://test:test@localhost/postgres
```

<ul><li>Prepare the connection parameters file ./sql/pg2pg.yaml</li></ul>

```yaml
threadCount: 10
initPGChunks: true
copyPGChunks: true

fromProperties:
  url: jdbc:postgresql://localhost:5432/postgres?options=-c%20enable_indexscan=off%20-c%20enable_indexonlyscan=off%20-c%20enable_bitmapscan=off
  user: test
  password: test
toProperties:
  url: jdbc:postgresql://localhost:5432/postgres
  user: test
  password: test
```


<ul><li>Prepare the table parameters file ./sql/pg2pg.json</li></ul>

```json
[
  {
    "fromSchemaName" : "PUBLIC",
    "fromTableName" : "SOURCE",
    "toSchemaName" : "PUBLIC",
    "toTableName" : "TARGET",
    "fetchWhereClause" : "1 = 1",
    "fromTaskName" : "TABLE1_TASK",
    "columnToColumn" : {
      "id"            : "id",
      "uuid"          : "uuid",
      "\"Primary\""   : "\"Primary\"",
      "boolean"       : "boolean",
      "int2"          : "int2",
      "int4"          : "int4",
      "int8"          : "int8",
      "smallint"      : "smallint",
      "bigint"        : "bigint",
      "numeric"       : "numeric",
      "float8"        : "float8",
      "date"          : "date",
      "timestamp"     : "timestamp",
      "timestamptz"   : "timestamptz",
      "description"   : "rem",
      "image"         : "image"
    }
  }
]
```

>  **WARNING**: The names of columns might be different at source and target

### Step 2
Halt any changes to the movable tables in the source database

### Step 3
Run the tool
```
java -jar ./target/bublik-1.2.jar -c ./sql/pg2pg.yaml -m ./sql/pg2pg.json
```

<ul><li>To prevent heap pressure, use `-Xmx16g`</li></ul>
<ul><li>Monitor the logs at `logs/app.log`.</li></ul>
<ul><li>Track progress at source:</li></ul>

```
select status, count(*), round(100 / sum(count(*)) over() * count(*),2) pct 
    from ctid_chunks group by status;
```
