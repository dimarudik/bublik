![Bublik](/sql/bublik.png)
# Tool for Data Transfer between databases


| SOURCE     | TARGET     |
|:-----------|:-----------|
| Oracle     | PostgreSQL |
| Oracle     | YDB        |
| PostgreSQL | PostgreSQL |
| PostgreSQL | YDB        |

This tool facilitates the efficient transfer of data between databases.<br>
The quickest method for extracting data from Oracle is by using `ROWID` (employing `dbms_parallel_execute` to segment the data into chunks). 
In case of PostgreSQL, we should split a table into chunks by `CTID` (PostgreSQL version >= 14). 
As you know, the fastest way to input data into PostgreSQL is through the `COPY` command in binary format.

* [Build](#Build)
* [Oracle To PostgreSQL](#Oracle-To-PostgreSQL)
  * [Prepare Oracle To PostgreSQL environment](#Prepare-Oracle-To-PostgreSQL-environment)
  * [Prepare Oracle To PostgreSQL Connection Settings](#Prepare-Oracle-To-PostgreSQL-Connection-Settings)
  * [Prepare Oracle To PostgreSQL Mapping File](#Prepare-Oracle-To-PostgreSQL-Mapping-File)
  * [Oracle To PostgreSQL Run](#Oracle-To-PostgreSQL-Run)
* [Oracle To YDB](#Oracle-To-YDB)
    * [Prepare Oracle To YDB environment](#Prepare-Oracle-To-YDB-environment)
    * [Prepare Oracle To YDB Connection Settings](#Prepare-Oracle-To-YDB-Connection-Settings)
    * [Prepare Oracle To YDB Mapping File](#Prepare-Oracle-To-YDB-Mapping-File)
    * [Oracle To YDB Run](#Oracle-To-YDB-Run)
* [PostgreSQL To PostgreSQL](#PostgreSQL-To-PostgreSQL)
  * [Prepare PostgreSQL To PostgreSQL environment](#Prepare-PostgreSQL-To-PostgreSQL-environment)
  * [Prepare PostgreSQL To PostgreSQL Connection Settings](#Prepare-PostgreSQL-To-PostgreSQL-Connection-Settings)
  * [Prepare PostgreSQL To PostgreSQL Mapping File](#Prepare-PostgreSQL-To-PostgreSQL-Mapping-File)
  * [PostgreSQL To PostgreSQL Run](#PostgreSQL-To-PostgreSQL-Run)
* [PostgreSQL To YDB](#PostgreSQL-To-YDB)
  * [Prepare PostgreSQL To YDB environment](#Prepare-PostgreSQL-To-YDB-environment)
  * [Prepare PostgreSQL To YDB Connection Settings](#Prepare-PostgreSQL-To-YDB-Connection-Settings)
  * [Prepare PostgreSQL To YDB Mapping File](#Prepare-PostgreSQL-To-YDB-Mapping-File)
  * [PostgreSQL To YDB Run](#PostgreSQL-To-YDB-Run)
* [Usage](#Usage)
  * [Usage as a service](#Usage-as-a-service)

## Build

Download the source code

```
git clone https://github.com/dimarudik/bublik.git
cd bublik/
```

[Install mvn](https://maven.apache.org/install.html)

Build and install all dependencies to local maven repository

```
mvn -f ./bublik-core/pom.xml clean install -DskipTests ; 
mvn -f ./bublik-cassandra/pom.xml clean install -DskipTests ; 
mvn -f ./bublik-postgres/pom.xml clean install -DskipTests ; 
mvn -f ./bublik-ydb/pom.xml clean install -DskipTests ; 
mvn -f ./bublik-oracle/pom.xml clean install -DskipTests; 
mvn -f ./bublik-cli/pom.xml clean install -DskipTests
```


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
| interval year to moth    | interval                                             |
| interval day to second   | interval                                             |

[Java Datatype Mappings](https://docs.oracle.com/en/database/oracle/oracle-database/23/jjdbc/accessing-and-manipulating-Oracle-data.html#GUID-1AF80C90-DFE6-4A3E-A407-52E805726778)

### Prepare Oracle To PostgreSQL environment

Build jar file for Oracle To PostgreSQL migration

```
mvn -f pom-oracleToPostgres.xml clean package -DskipTests
```

[Use Java >= 21](https://jdk.java.net/archive/)

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
        -h postgres \
        -e POSTGRES_USER=postgres \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=postgres \
        -p 5432:5432 \
        -v ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql \
        -v ./sql/.psqlrc:/var/lib/postgresql/.psqlrc \
        -v ./sql/bublik.png:/var/lib/postgresql/bublik.png \
        -d postgres \
        -c shared_preload_libraries="pg_stat_statements,auto_explain" \
        -c timezone="+03" \
        -c max_connections=200 \
        -c logging_collector=on \
        -c log_directory=pg_log \
        -c log_filename=%u_%a.log \
        -c log_min_duration_statement=3 \
        -c log_statement=all \
        -c wal_level=logical \
        -c auto_explain.log_min_duration=0 \
        -c auto_explain.log_analyze=true
```

>  **WARNING**: Tables `public.table1`, `public.table2`, `public.parted` will be created during postgre docker container startup

How to connect to PostgreSQL:

```
psql postgresql://test:test@localhost/postgres
```

### Prepare Oracle To PostgreSQL Connection Settings

You can run the tool by using yaml with connection settings:

##### ./bublik-cli/config/ora2pg.yaml

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

Or you can use environment variables (do not specify -c parameter):

```
export THREAD_COUNT=10
export FROM_URL=oracle:thin:@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=ORCLPDB1)))
export FROM_USER=test
export FROM_PASSWORD=test
export TO_URL=jdbc:postgresql://localhost:5432/postgres
export TO_USER=test
export TO_PASSWORD=test
```

```
java -jar ./target/bublik-25.1.0.jar -k 50000 -c -m ./bublik-cli/config/ora2pg.json
```

### Prepare Oracle To PostgreSQL Mapping File

##### ./bublik-cli/config/ora2pg.json

```json
[
  {
    "fromSchemaName" : "TEST",
    "fromTableName" : "TABLE1",
    "fromTableAlias" : "t",
    "fromTableAdds" : "left join test.currencies c on t.currency_id = c.id",
    "toSchemaName" : "PUBLIC",
    "toTableName" : "TABLE1",
    "fetchHintClause" : "/*+ no_index(T) */",
    "fetchWhereClause" : "1 = 1",
    "fromTaskName" : "TABLE1_TASK",
    "fromTaskWhereClause" : " 1 = 1 ",
    "tryCharIfAny" : ["current_mood"],
    "columnToColumn" : {
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
      "t.id as id" : "id",
      "c.name as currency_name" : "currency_name",
      "(select name from test.countries c where c.id = t.country_id) as country_name" : "country_name"
    }
  },
  {
    "fromSchemaName" : "TEST",
    "fromTableName" : "\"Table2\"",
    "toSchemaName" : "PUBLIC",
    "toTableName" : "\"TABLE2\"",
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
    },
    "columnFromMany" : {
       "tstzrange" : ["create_at", "update_at"]
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
    "fromTaskWhereClause" : "(DBMS_ROWID.ROWID_OBJECT(START_ROWID) IN ((select DBMS_ROWID.ROWID_OBJECT(rowid) object_id from test.parted partition for (to_date('20220101', 'YYYYMMDD')) where rownum = 1), (select DBMS_ROWID.ROWID_OBJECT(rowid) object_id from test.parted partition for (to_date('20230101', 'YYYYMMDD')) where rownum = 1)) OR DBMS_ROWID.ROWID_OBJECT(END_ROWID) IN ((select DBMS_ROWID.ROWID_OBJECT(rowid) object_id from test.parted partition for (to_date('20220101', 'YYYYMMDD')) where rownum = 1),(select DBMS_ROWID.ROWID_OBJECT(rowid) object_id from test.parted partition for (to_date('20230101', 'YYYYMMDD')) where rownum = 1)))",
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
> To enrich data from other tables you can use combination of <br>
> **fromTableAlias**, **fromTableAdds** and **expressionToColumn** definitions <br> 
> In example with TABLE1 the data will be retrieved by query:

 > ```
 > SELECT /* bublik */ /*+ no_index(T) */
 >   "LEVEL",
 >   create_at,
 >   update_at,
 >   gender,
 >   byteablob,
 >   textclob,
 >   "CaseSensitive",
 >   rawbytea,
 >   doc,
 >   uuid,
 >   clobjsonb,
 >   current_mood,
 >   t.id as id,
 >   c.name as currency_name,
 >   (select name from test.countries c where c.id = t.country_id) as country_name
 > FROM TEST.TABLE1 t left join test.currencies c
 >   on t.currency_id = c.id WHERE 1 = 1 and t.rowid between ? and ?
 > ```


> [!NOTE]
> To speed up the chunk processing of partitioned table you can apply **fromTaskWhereClause** clause as it used above.
> It allows to exclude excessive workload

> [!NOTE]
> If the target column type doesn't support by tool you can try to use Character  
> by using declaration of column's name in **tryCharIfAny** array
 
### Oracle To PostgreSQL Run

Halt any changes to the movable tables in the source database (Oracle) and run:

```
java -jar ./target/bublik-25.1.0.jar -k 50000 -c ./bublik-cli/config/ora2pg.yaml -m ./bublik-cli/config/ora2pg.json
```

Chunks will be created automatically with parameter -k at startup

> [!NOTE]
> If the migration was interrupted due to any infrastructure issues you can resume the process without -k parameter.
> In this case unprocessed chunks of data will be transfer 


## Oracle To YDB

The objective is to migrate table <strong>likes</strong> to table <strong>likes_all</strong> from PostgreSQL to YDB with enrichment of data from other tables.

### Prepare Oracle To YDB environment

Build jar file for Oracle To PostgreSQL migration

```
mvn -f pom-oracleToYdb.xml clean package -DskipTests
```

[Use Java >= 21](https://jdk.java.net/archive/)

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

#### Prepare YDB environment

Do the next steps:

```shell
mkdir ~/ydbd && cd ~/ydbd
mkdir ydb_data
mkdir ydb_certs
```

```shell
docker run -d --rm --name ydb-local -h localhost \
  --platform linux/amd64 \
  -p 2135:2135 -p 2136:2136 -p 8765:8765 -p 9092:9092 \
  -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
  -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
  -e YDB_KAFKA_PROXY_PORT=9092 \
  ydbplatform/local-ydb:latest
```

```shell
curl -sSL https://install.ydb.tech/cli | bash
exec -l $SHELL
```

```shell
ydb -e grpc://localhost:2136 -d /local yql -s 'create table `likes_all` (id Uint64, user_id Uint64, item_id Uint64, user_name bytes, email bytes, item_name bytes, description bytes, primary key (id));'
```

<ul><li>How to connect to YDB</li></ul>

```
ydb -e grpc://localhost:2136 -d /local
```

### Prepare Oracle To YDB Connection Settings

You can run the tool by using yaml with connection settings:

##### ./bublik-cli/config/ora2ydb.yaml

```yaml
threadCount: 4

fromProperties:
  url: jdbc:oracle:thin:@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=ORCLPDB1)))
  user: test
  password: test
toProperties:
  url: jdbc:ydb:grpc://localhost:2136/local
  user: ""
  password: ""
```

Or you can use environment variables (do not specify -c parameter):

```
export THREAD_COUNT=10
export FROM_URL=oracle:thin:@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=ORCLPDB1)))
export FROM_USER=test
export FROM_PASSWORD=test
export TO_URL=jdbc:ydb:grpc://localhost:2136/local
export TO_USER=""
export TO_PASSWORD=""
```

### Prepare Oracle To YDB Mapping File

##### ./bublik-cli/config/ora2ydb.json

```json
[
  {
    "fromSchemaName" : "test",
    "fromTableName" : "likes",
    "fromTableAlias" : "l",
    "fromTableAdds" : "left join users u on u.id = l.user_id left join items i on i.id = l.item_id",
    "toSchemaName" : "",
    "toTableName" : "likes_all",
    "expressionToColumn" : {
      "l.id as id"                    : "id",
      "l.user_id as user_id"          : "user_id",
      "l.item_id as item_id"          : "item_id",
      "u.user_name as user_name"      : "user_name",
      "u.email as email"              : "email",
      "i.item_name as item_name"      : "item_name",
      "i.description as description"  : "description"
    }
  }
]
```

> [!IMPORTANT]
> The case-sensitive or reserved words must be quoted with double quotation and backslashes

> [!NOTE]
> To enrich data from other tables you can use combination of <br>
> **fromTableAlias**, **fromTableAdds** and **expressionToColumn** definitions <br>
> In example with TABLE1 the data will be retrieved by query:

> ```
 > select /* bublik */ /*+ no_index(l) */ 
 > 	l.user_id as user_id,
 > 	u.email as email,
 > 	l.id as id,
 > 	u.user_name as user_name,
 > 	l.item_id as item_id,
 > 	i.description as description,
 > 	i.item_name as item_name 
 > from test.likes l 
 > left join users u on u.id = l.user_id 
 > left join items i on i.id = l.item_id 
 > where ( 1 = 1 ) and l.rowid between ? and ?
 > ```

### Oracle To YDB Run

Halt any changes to the movable tables in the source database (Oracle) and run:

```
java -jar ./target/bublik-25.1.0.jar -k 50000 -c ./bublik-cli/config/ora2ydb.yaml -m ./bublik-cli/config/ora2ydb.json
```

Chunks will be created automatically with parameter -k at startup

> [!NOTE]
> If the migration was interrupted due to any infrastructure issues you can resume the process without -k parameter.
> In this case unprocessed chunks of data will be transfer


## PostgreSQL To PostgreSQL
![PostgreSQL To PostgreSQL](/sql/PostgreSQLToPostgreSQL.png)

The objective is to migrate table <strong>Source</strong> to table <strong>target</strong> from one PostgreSQL database to another. To simplify test case we're using same database


### Prepare PostgreSQL To PostgreSQL environment

> [!NOTE]
> Bublik uses Tid Range Scan to retrieve data, however this access method has been implemented in PostgreSQL 14.0 and later.
> Therefore please use PostgreSQL >= 14.0 at source side

[E.18.3.1.4. Optimizer](https://www.postgresql.org/docs/14/release-14.html#id-1.11.6.23.5)


All activities are reproducible in docker containers

Build jar file for PostgreSQL To PostgreSQL migration

```
mvn -f pom-postgresToPostgres.xml clean package -DskipTests
```

[Use Java >= 21](https://jdk.java.net/archive/)


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
        -c wal_level=logical \
        -c auto_explain.log_min_duration=0 \
        -c auto_explain.log_analyze=true
```

>  **WARNING**: SOURCE & TARGET tables will be created during postgre docker container startup

<ul><li>How to connect</li></ul>

```
psql postgresql://test:test@localhost/postgres
```

### Prepare PostgreSQL To PostgreSQL Connection Settings

You can run the tool by using yaml with connection settings:

```yaml
threadCount: 10

fromProperties:
  url: jdbc:postgresql://localhost:5432/postgres?options=-c%20enable_indexscan=off%20-c%20enable_indexonlyscan=off%20-c%20enable_bitmapscan=off
  user: test
  password: test
toProperties:
  url: jdbc:postgresql://localhost:5432/postgres
  user: test
  password: test
```

Or you can use environment variables (do not specify -c parameter):

```
export THREAD_COUNT=10
export FROM_URL=jdbc:postgresql://localhost:5432/postgres?options=-c%20enable_indexscan=off%20-c%20enable_indexonlyscan=off%20-c%20enable_bitmapscan=off
export FROM_USER=test
export FROM_PASSWORD=test
export TO_URL=jdbc:postgresql://localhost:5432/postgres
export TO_USER=test
export TO_PASSWORD=test
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
    "tryCharIfAny" : ["current_mood", "gender"],
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
    },
    "expressionToColumn" : {
      "(select 'male') as gender" : "gender"
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

### PostgreSQL To PostgreSQL Run

Halt any changes to the movable tables in the source database and run:

```
java -jar ./target/bublik-25.1.0.jar -k 50000 -c ./bublik-cli/config/pg2pg.yaml -m ./bublik-cli/config/pg2pg.json
```

Chunks will be created automatically with parameter -k at startup

> [!NOTE]
> If the migration was interrupted due to any infrastructure issues you can resume the process without -k parameter.
> In this case unprocessed chunks of data will be transfer

> [!IMPORTANT]
> Due to chunk creation based on statistics of the table
> please check that ANALYZE is performed on regular basis


## PostgreSQL To YDB
![PostgreSQL To PostgreSQL](/sql/PostgreSQLToPostgreSQL.png)

The objective is to migrate table <strong>likes</strong> to table <strong>likes_all</strong> from PostgreSQL to YDB with enrichment of data from other tables.


### Prepare PostgreSQL To YDB environment

> [!NOTE]
> Bublik uses Tid Range Scan to retrieve data, however this access method has been implemented in PostgreSQL 14.0 and later.
> Therefore please use PostgreSQL >= 14.0 at source side

[E.18.3.1.4. Optimizer](https://www.postgresql.org/docs/14/release-14.html#id-1.11.6.23.5)


All activities are reproducible in docker containers

Build jar file for PostgreSQL to YDB migration

```
mvn -f pom-postgresToYdb.xml clean package -DskipTests
```

[Use Java >= 21](https://jdk.java.net/archive/)


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
        -c wal_level=logical \
        -c auto_explain.log_min_duration=0 \
        -c auto_explain.log_analyze=true
```

<ul><li>How to connect</li></ul>

```
psql postgresql://test:test@localhost/postgres
```

[YDB Quick Start](https://ydb.tech/docs/en/quickstart?tabs=defaultTabsGroup-3dol9c63_docker%2520x86_64)

Do the next steps to prepare YDB environment:

```shell
mkdir ~/ydbd && cd ~/ydbd
mkdir ydb_data
mkdir ydb_certs
```

```shell
docker run -d --rm --name ydb-local -h localhost \
  --platform linux/amd64 \
  -p 2135:2135 -p 2136:2136 -p 8765:8765 -p 9092:9092 \
  -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
  -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
  -e YDB_KAFKA_PROXY_PORT=9092 \
  ydbplatform/local-ydb:latest
```

```shell
curl -sSL https://install.ydb.tech/cli | bash
exec -l $SHELL
```

```shell
ydb -e grpc://localhost:2136 -d /local yql -s 'create table `likes_all` (id Uint64, user_id Uint64, item_id Uint64, user_name bytes, email bytes, item_name bytes, description bytes, primary key (id));'
```

<ul><li>How to connect to YDB</li></ul>

```
ydb -e grpc://localhost:2136 -d /local
```

### Prepare PostgreSQL To YDB Connection Settings

You can run the tool by using yaml with connection settings:

```yaml
threadCount: 4

fromProperties:
  url: jdbc:postgresql://localhost:5432/postgres?options=-c%20enable_indexscan=off%20-c%20enable_indexonlyscan=off%20-c%20enable_bitmapscan=off
  user: test
  password: test
toProperties:
  url: jdbc:ydb:grpc://localhost:2136/local
  user: ""
  password: ""
```

Or you can use environment variables (do not specify -c parameter):

```
export THREAD_COUNT=4
export FROM_URL=jdbc:postgresql://localhost:5432/postgres?options=-c%20enable_indexscan=off%20-c%20enable_indexonlyscan=off%20-c%20enable_bitmapscan=off
export FROM_USER=test
export FROM_PASSWORD=test
export TO_URL=jdbc:ydb:grpc://localhost:2136/local
export TO_USER=""
export TO_PASSWORD="
```

### Prepare PostgreSQL To YDB Mapping File

In this example we will enrich data from other tables

```json
[
  {
    "fromSchemaName" : "public",
    "fromTableName" : "likes",
    "fromTableAlias" : "l",
    "fromTableAdds" : "left join users u on u.id = l.user_id left join items i on i.id = l.item_id",
    "toSchemaName" : "",
    "toTableName" : "likes_all",
    "fetchWhereClause" : "1 = 1",
    "fromTaskName" : "likes_all",
    "expressionToColumn" : {
      "l.id as id"                    : "id",
      "l.user_id as user_id"          : "user_id",
      "l.item_id as item_id"          : "item_id",
      "u.user_name as user_name"      : "user_name",
      "u.email as email"              : "email",
      "i.item_name as item_name"      : "item_name",
      "i.description as description"  : "description"
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

### PostgreSQL To YDB Run

Halt any changes to the movable tables in the source database and run:

```
java -jar ./target/bublik-25.1.0.jar -k 50000 -c ./bublik-cli/config/pg2ydb.yaml -m ./bublik-cli/config/pg2ydb.json
```

Chunks will be created automatically with parameter -k at startup

> [!NOTE]
> If the migration was interrupted due to any infrastructure issues you can resume the process without -k parameter.
> In this case unprocessed chunks of data will be transfer

> [!IMPORTANT]
> Due to chunk creation based on statistics of the table
> please check that ANALYZE is performed on regular basis



## PostgreSQL To Cassandra (development)

![Cassandra](/sql/cassandra4.png)

[Java Datatype Mappings](https://documentation.softwareag.com/webmethods/adapters_estandards/Adapters/Apache_Cassandra/Apache_for_Cassandra_10-2/10-2-0_Apache_Cassandra_webhelp/index.html#page/cassandra-webhelp/co-cql_data_type_to_jdbc_data_type.html)

### Prepare PostgreSQL To Cassandra environment

```shell
docker network create \
  --driver=bridge \
  --subnet=172.28.0.0/16 \
  --gateway=172.28.5.254 \
  bublik-network
```

```shell
docker run \
        --name postgres \
        --ip 172.28.0.7 \
        -h postgres \
        --network bublik-network \
        -e POSTGRES_USER=postgres \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=postgres \
        -p 5432:5432 \
        -v ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql \
        -v ./sql/.psqlrc:/var/lib/postgresql/.psqlrc \
        -v ./sql/bublik.png:/var/lib/postgresql/bublik.png \
        -d postgres \
        -c shared_preload_libraries="pg_stat_statements,auto_explain" \
        -c timezone="+03" \
        -c max_connections=200 \
        -c logging_collector=on \
        -c log_directory=pg_log \
        -c log_filename=%u_%a.log \
        -c log_min_duration_statement=3 \
        -c log_statement=all \
        -c auto_explain.log_min_duration=0 \
        -c auto_explain.log_analyze=true
```

```shell
docker build ./dockerfiles/cs1 -t cs1 ; \
docker build ./dockerfiles/cs2 -t cs2 ; \
docker build ./dockerfiles/cs3 -t cs3 ; \
docker build ./dockerfiles/cs4 -t cs4 ; \
docker build ./dockerfiles/cs5 -t cs5 ; \
docker build ./dockerfiles/cs6 -t cs6
```

```shell
docker run -d -h cs1 --ip 172.28.0.1 --name cs1 --network bublik-network -p 9042:9042 cs1 ; \
sleep 15; docker run -d -h cs2 --ip 172.28.0.2 --name cs2 --network bublik-network cs2 ; \
sleep 45; docker run -d -h cs3 --ip 172.28.0.3 --name cs3 --network bublik-network cs3 ; \
sleep 45; docker run -d -h cs4 --ip 172.28.0.4 --name cs4 --network bublik-network cs4 ; \
sleep 45; docker run -d -h cs5 --ip 172.28.0.5 --name cs5 --network bublik-network cs5 ; \
sleep 45; docker run -d -h cs6 --ip 172.28.0.6 --name cs6 --network bublik-network cs6
```

> [!IMPORTANT]
> Wait until all nodes start. To check the status you can use nodetool as shown below.
> If a node fails to start, remove node like: 
> docker exec cs1 nodetool assassinate IP 
> and re-create the broken container 

```shell
docker exec cs1 nodetool status
# or
docker exec cs1 nodetool describecluster
```

Adjust the ``batch_size_fail_threshold_in_kb`` parameter

```shell
docker exec -it cs1 nodetool sjk mx -ms -b org.apache.cassandra.db:type=StorageService -f BatchSizeFailureThreshold -v 1024 ; \
docker exec -it cs2 nodetool sjk mx -ms -b org.apache.cassandra.db:type=StorageService -f BatchSizeFailureThreshold -v 1024 ; \
docker exec -it cs3 nodetool sjk mx -ms -b org.apache.cassandra.db:type=StorageService -f BatchSizeFailureThreshold -v 1024 ; \
docker exec -it cs4 nodetool sjk mx -ms -b org.apache.cassandra.db:type=StorageService -f BatchSizeFailureThreshold -v 1024 ; \
docker exec -it cs5 nodetool sjk mx -ms -b org.apache.cassandra.db:type=StorageService -f BatchSizeFailureThreshold -v 1024 ; \
docker exec -it cs6 nodetool sjk mx -ms -b org.apache.cassandra.db:type=StorageService -f BatchSizeFailureThreshold -v 1024
```

Prepare the Keyspace and tables

```shell
cqlsh -u cassandra -p cassandra -f ./sql/data.cql
```

```shell
docker exec -it cs1 nodetool repair ; \
docker exec -it cs2 nodetool repair ; \
docker exec -it cs3 nodetool repair ; \
docker exec -it cs4 nodetool repair ; \
docker exec -it cs5 nodetool repair ; \
docker exec -it cs6 nodetool repair
```

```shell
mvn -f bublik/pom.xml clean install -DskipTests ; \ 
mvn -f cli/pom.xml clean package -DskipTests ; \
psql postgresql://test:test@localhost/postgres -c "drop table ctid_chunks" ; \
docker rm cli -f ; \
docker image rm cli ; \
docker rmi $(docker images -f "dangling=true" -q) ; \
docker volume prune -f ; \
docker build --no-cache -t cli . ; \
docker run -h cli --network bublik-network --name cli cli:latest
```

## Usage

![Bublik](/sql/bublik.png)

Bublik library might be used as standalone utility or as a part of service

### Usage as a service

Build the service (example)

```shell
cd ./service
./gradlew clean build -x test
```

Halt any changes to the movable tables in the source database

Run the service:

```
java -jar ./build/libs/service-25.1.0.jar
```

Consume the service:

```shell
newman run ./postman/postman_collection.json
```

