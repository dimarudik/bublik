![Bublik](/sql/bublik.png)
# Tool for Data Transfer from Oracle to PostgreSQL or from PostgreSQL to PostgreSQL

This tool facilitates the efficient transfer of data from Oracle to PostgreSQL or from PostgreSQL to PostgreSQL.<br>
The quickest method for extracting data from Oracle is by using `ROWID` (employing `dbms_parallel_execute` to segment the data into chunks). 
In case of PostgreSQL, we should split a table into chunks by `CTID`.<br>
As you know, the fastest way to input data into PostgreSQL is through the `COPY` command in binary format.

* [Oracle To PostgreSQL](#Oracle-To-PostgreSQL)
  * [Prepare Oracle To PostgreSQL environment](#Prepare-Oracle-To-PostgreSQL-environment)
  * [Prepare Oracle To PostgreSQL Config File](#Prepare-Oracle-To-PostgreSQL-Config-File)
  * [Prepare Oracle To PostgreSQL Mapping File](#Prepare-Oracle-To-PostgreSQL-Mapping-File)
  * [Create chunks](#Create-chunks)
* [PostgreSQL To PostgreSQL](#PostgreSQL-To-PostgreSQL)
  * [Prepare PostgreSQL To PostgreSQL environment](#Prepare-PostgreSQL-To-PostgreSQL-environment)
  * [Prepare PostgreSQL To PostgreSQL Config File](#Prepare-PostgreSQL-To-PostgreSQL-Config-File)
  * [Prepare PostgreSQL To PostgreSQL Mapping File](#Prepare-PostgreSQL-To-PostgreSQL-Mapping-File)
  * [Create CTID chunks](#Create-CTID-chunks)
* [Usage](#Usage)
  * [Usage as a cli](#Usage-as-a-cli)
  * [Usage as a service](#Usage-as-a-service)

## Oracle To PostgreSQL
![Oracle To PostgreSQL](/sql/oracletopostgresql.png)

The objective is to migrate tables <strong>TABLE1</strong>, <strong>Table2</strong>, <strong>PARTED</strong> from Oracle schema <strong>TEST</strong> to a PostgreSQL database.

**Supported types:**

| ORACLE                   | Postgresql (possible types)                          |
|:-------------------------|:-----------------------------------------------------|
| char, varchar, varchar2  | char, bpchar, varchar, text, uuid                    |
| varchar2                 | jsonb                                                |
| CLOB                     | varchar, text, jsonb                                 |
| BLOB                     | bytea                                                |
| RAW                      | bytea                                                |
| date                     | date, timestamp, timestamptz                         |
| timestamp                | timestamp, timestamptz                               |
| timestamp with time zone | timestamptz                                          |
| number                   | numeric, smallint, bigint, integer, double precision |

[Java Datatype Mappings](https://docs.oracle.com/en/database/oracle/oracle-database/23/jjdbc/accessing-and-manipulating-Oracle-data.html#GUID-1AF80C90-DFE6-4A3E-A407-52E805726778)

### Prepare Oracle To PostgreSQL environment

All activities are reproducible in docker containers

```
git clone https://github.com/dimarudik/bublik.git
cd bublik/
```

#### Prepare Oracle environment

- arm64:

  > ```
  > docker run --name oracle \
  > -p 1521:1521 -p 5500:5500 \
  >     -e ORACLE_PWD=oracle_4U \
  >     -v ./dockerfiles/scripts:/docker-entrypoint-initdb.d \
  >     -d dimarudik/oracle_arm64:19.3.0-ee
  > ```

- x86_64:

  > ```
  > docker run --name oracle \
  >     -p 1521:1521 -p 5500:5500 \
  >     -e ORACLE_PWD=oracle_4U \
  >     -v ./dockerfiles/scripts:/docker-entrypoint-initdb.d \
  >     -d dimarudik/oracle_x86_64:19.3.0-ee
  > ```
  
>  **WARNING**: Tables `TABLE1`, `Table2`, `PARTED` will be created and fulfilled during oracle docker container startup

How to connect to Oracle:

```
sqlplus 'test/test@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=ORCLPDB1)))'
```

> [!NOTE]
> [How to install Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html)

#### Prepare PostgreSQL environment

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

>  **WARNING**: Tables `public.table1`, `public.table2`, `public.parted` will be created during postgre docker container startup

How to connect to PostgreSQL:

```
psql postgresql://test:test@localhost/postgres
```

### Prepare Oracle To PostgreSQL Config File 

##### ./sql/ora2pg.yaml

  > ```yaml
  > threadCount: 10
  > 
  > fromProperties:
  >   url: jdbc:oracle:thin:@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=ORCLPDB1)))
  >   user: test
  >   password: test
  > toProperties:
  >   url: jdbc:postgresql://localhost:5432/postgres
  >   user: test
  >   password: test
  > ```


### Prepare Oracle To PostgreSQL Mapping File 

##### ./sql/ora2pg.json

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
    "fromTaskWhereClause" : " 1 = 1 ",
    "tryCharIfAny" : ["current_mood"],
    "columnToColumn" : {
      "id"                : "id",
      "\"LEVEL\""         : "level",
      "create_at"         : "create_at",
      "update_at"         : "update_at",
      "gender"            : "gender",
      "byteablob"         : "byteablob",
      "textclob"          : "textclob",
      "\"CaseSensitive\"" : "\"CaseSensitive\"",
      "rawbytea"          : "rawbytea",
      "doc"               : "doc",
      "uuid"              : "uuid",
      "clobjsonb"         : "clobjsonb",
      "current_mood"      : "current_mood"
    },
    "expressionToColumn" : {
      "(select name from test.countries where countries.id = table1.country_id) as country_name" : "country_name"
    }
  },
  {
    "fromSchemaName" : "TEST",
    "fromTableName" : "\"Table2\"",
    "toSchemaName" : "PUBLIC",
    "toTableName" : "TABLE2",
    "fetchHintClause" : "/*+ no_index(TABLE2) */",
    "fetchWhereClause" : "1 = 1",
    "fromTaskName" : "TABLE2_TASK",
    "columnToColumn" : {
      "id"          : "id",
      "\"LEVEL\""   : "level",
      "create_at"   : "create_at",
      "update_at"   : "update_at",
      "gender"      : "gender",
      "byteablob"   : "byteablob",
      "textclob"    : "textclob"
    }
  },
  {
    "fromSchemaName" : "TEST",
    "fromTableName" : "PARTED",
    "toSchemaName" : "PUBLIC",
    "toTableName" : "PARTED",
    "fetchHintClause" : "/*+ no_index(PARTED) */",
    "fetchWhereClause" : "create_at >= to_date('2022-01-01','YYYY-MM-DD') and create_at <= to_date('2023-12-31','YYYY-MM-DD')",
    "fromTaskName" : "PARTED_TASK",
    "fromTaskWhereClause" : "DBMS_ROWID.ROWID_OBJECT(START_ROWID) IN (73021,73022) OR DBMS_ROWID.ROWID_OBJECT(END_ROWID) IN (73021,73022)",
    "columnToColumn" : {
      "id"        : "id",
      "create_at" : "create_at",
      "name"      : "name"
    }
  }
]
```

> [!IMPORTANT]
> The case-sensitive or reserved words must be quoted with double quotation and backslashes  

> [!NOTE]
> **expressionToColumn** might be used for declaration of subquery for enrichment of data 

> [!NOTE]
> To speed up the chunk processing of partitioned table you can apply **fromTaskWhereClause** clause as it used above.
> It allows to exclude excessive workload

> [!NOTE]
> If the target column type doesn't support by tool you can try to use Character  
> by using declaration of column's name in **tryCharIfAny** array
 
### Create chunks

Halt any changes to the movable tables in the source database (Oracle)<br>
Prepare data chunks in Oracle using the same user credentials specified in `bublik` tool (`fromProperties` in `./sql/ora2pg.yaml`):

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
exec dbms_parallel_execute.drop_task(task_name => 'PARTED_TASK');
exec dbms_parallel_execute.create_task(task_name => 'PARTED_TASK');
begin
    dbms_parallel_execute.create_chunks_by_rowid (  task_name   => 'PARTED_TASK',
                                                    table_owner => 'TEST',
                                                    table_name  => 'PARTED',
                                                    by_row => TRUE,
                                                    chunk_size  => 20000 );
end;
/
```

> [!NOTE]
> [How to build and run the tool](#Build-the-jar)

## PostgreSQL To PostgreSQL
![PostgreSQL To PostgreSQL](/sql/PostgreSQLToPostgreSQL.png)

The objective is to migrate table <strong>Source</strong> to table <strong>target</strong> from one PostgreSQL database to another. To simplify test case we're using same database


### Prepare PostgreSQL To PostgreSQL environment

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


### Prepare PostgreSQL To PostgreSQL Config File

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


### Prepare PostgreSQL To PostgreSQL Mapping File

```json
[
  {
    "fromSchemaName" : "PUBLIC",
    "fromTableName" : "\"Source\"",
    "toSchemaName" : "PUBLIC",
    "toTableName" : "TARGET",
    "fetchWhereClause" : "1 = 1",
    "fromTaskName" : "TABLE1_TASK",
    "tryCharIfAny" : ["current_mood"],
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
      "image"         : "image",
      "current_mood"  : "current_mood"
    }
  }
]
```

> [!IMPORTANT]
> The case-sensitive or reserved words must be quoted with double quotation and backslashes

> [!NOTE]
> **expressionToColumn** might be used for declaration of subquery for enrichment of data

> [!NOTE]
> If the target column type doesn't support by tool you can try to use Character  
> by using declaration of column's name in **tryCharIfAny** array

### Create CTID chunks

To begin the transferring of data from source to target you should prepare the CTID table fulfilled by info of chunks

```
create table if not exists public.ctid_chunks (
    chunk_id int generated always as identity primary key,
    start_page bigint,
    end_page bigint,
    task_name varchar(128),
    status varchar(20)  default 'UNASSIGNED',
    unique (start_page, end_page, task_name, status));
```

> [!IMPORTANT]
> If parameter **initPGChunks** has the true value, the CTID table will be created and fulfilled automatically

## Usage

![Bublik](/sql/bublik.png)

Bublik library might be used as a part of cli utility or as a part of service

Before usage build the jar and put it in a local maven repository

```shell
cd ./bublik
mvn clean install -DskipTests
```

### Usage as a cli

Build the cli

```shell
cd ./cli
mvn clean package -DskipTests
```

Halt any changes to the movable tables in the source database.

Run the cli:

- Oracle:
  > ```
  > java -jar ./target/bublik-cli-1.2.0.jar -c ./config/ora2pg.yaml -m ./config/ora2pg.json
  > ```
- PostgreSQL
  > ```
  > java -jar ./target/bublik-cli-1.2.0.jar -c ./sql/pg2pg.yaml -m ./sql/pg2pg.json
  > ```

- To prevent heap pressure, use `-Xmx16g`
- Monitor the logs at `logs/app.log`
- Track progress in Oracle:
  > ```
  > select status, count(*), round(100 / sum(count(*)) over() * count(*),2) pct 
  >     from user_parallel_execute_chunks group by status;
  > ```
- Track progress in PostgreSQL:
  > ```
  > select status, count(*), round(100 / sum(count(*)) over() * count(*),2) pct 
  >     from ctid_chunks group by status;
  > ```

### Usage as a service

Build the service

```shell
cd ./service
./gradlew clean build -x test
```

Halt any changes to the movable tables in the source database

Run the service:

```java
java -jar ./build/libs/service-1.2.0.jar
```

Consume the service:

```shell
newman run ./postman/postman_collection.json
```