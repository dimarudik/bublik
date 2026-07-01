![Bublik](/sql/bublik.png)
# Tool for Data Transfer between databases


| TO ➔ <br> FROM ⬇ | Cassandra | ClickHouse | MS SQL | Oracle | PostgreSQL | YDB |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Cassandra** | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ |
| **ClickHouse** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **MS SQL** | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ |
| **Oracle** | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ |
| **PostgreSQL** | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ |
| **YDB** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |


This tool facilitates the efficient transfer of data between databases.
* The quickest method for extracting data from <strong>Oracle</strong> is by using `ROWID` (employing `dbms_parallel_execute` to segment the data into chunks). 
* In case of <strong>PostgreSQL</strong>, we should split a table into chunks by `CTID` (PostgreSQL version >= 14). As you know, the fastest way to input data into PostgreSQL is through the `COPY` command in binary format.
* If you are using <strong>Cassandra</strong>, you can split the data into chunks based on Token Ranges.
* When you are transferring data from <strong>MS SQL</strong> to <strong>PostgreSQL</strong>, the only way to speed up the process is to rely on the clustering key of SQL Server table.

You can find more details and examples below.

* [Build](#Build)
* [Cassandra To Cassandra](#cassandra-to-cassandra)
    * [Prepare Cassandra To Cassandra environment](#prepare-cassandra-to-cassandra-environment)
    * [Prepare Cassandra To Cassandra Connection Settings](#prepare-cassandra-to-cassandra-connection-settings)
    * [Prepare Cassandra To Cassandra Mapping Files](#prepare-cassandra-to-cassandra-mapping-files)
    * [Cassandra To Cassandra Run](#cassandra-to-cassandra-run)
* [Cassandra To PostgreSQL](#cassandra-to-postgresql)
    * [Prepare Cassandra To PostgreSQL environment](#prepare-cassandra-to-postgresql-environment)
    * [Prepare Cassandra To PostgreSQL Connection Settings](#prepare-cassandra-to-postgresql-connection-settings)
    * [Prepare Cassandra To PostgreSQL Mapping Files](#prepare-cassandra-to-postgresql-mapping-files)
    * [Cassandra To PostgreSQL Run](#cassandra-to-postgresql-run)
* [MS SQL To PostgreSQL](#ms-sql-to-postgresql)
  * [Prepare MS SQL To PostgreSQL environment](#prepare-ms-sql-to-postgresql-environment)
  * [Prepare MS SQL To PostgreSQL Connection Settings](#prepare-ms-sql-to-postgresql-connection-settings)
  * [Prepare MS SQL To PostgreSQL Mapping File](#prepare-ms-sql-to-postgresql-mapping-file)
  * [MS SQL To PostgreSQL Run](#ms-sql-to-postgresql-run)
* [Oracle To Cassandra](#oracle-to-cassandra)
    * [Prepare Oracle To Cassandra environment](#prepare-oracle-to-cassandra-environment)
    * [Prepare Oracle To Cassandra Connection Settings](#prepare-oracle-to-cassandra-connection-settings)
    * [Prepare Oracle To Cassandra Mapping File](#prepare-oracle-to-cassandra-mapping-file)
    * [Oracle To Cassandra Run](#oracle-to-cassandra-run)
* [Oracle To PostgreSQL](#oracle-to-postgresql)
  * [Prepare Oracle To PostgreSQL environment](#prepare-oracle-to-postgresql-environment)
  * [Prepare Oracle To PostgreSQL Connection Settings](#prepare-oracle-to-postgresql-connection-settings)
  * [Prepare Oracle To PostgreSQL Mapping File](#prepare-oracle-to-postgresql-mapping-file)
  * [Oracle To PostgreSQL Run](#oracle-to-postgresql-run)
* [Oracle To YDB](#oracle-to-ydb)
    * [Prepare Oracle To YDB environment](#prepare-oracle-to-ydb-environment)
    * [Prepare Oracle To YDB Connection Settings](#prepare-oracle-to-ydb-connection-settings)
    * [Prepare Oracle To YDB Mapping File](#prepare-oracle-to-ydb-mapping-file)
    * [Oracle To YDB Run](#oracle-to-ydb-run)
* [PostgreSQL To Cassandra](#postgresql-to-cassandra)
    * [Prepare PostgreSQL To Cassandra environment](#prepare-postgresql-to-cassandra-environment)
    * [Prepare PostgreSQL To Cassandra Connection Settings](#prepare-postgresql-to-cassandra-connection-settings)
    * [Prepare PostgreSQL To Cassandra Mapping File](#prepare-postgresql-to-cassandra-mapping-file)
    * [PostgreSQL To Cassandra Run](#postgresql-to-cassandra-run)
* [PostgreSQL To PostgreSQL](#postgresql-to-postgresql)
  * [Prepare PostgreSQL To PostgreSQL environment](#prepare-postgresql-to-postgresql-environment)
  * [Prepare PostgreSQL To PostgreSQL Connection Settings](#prepare-postgresql-to-postgresql-connection-settings)
  * [Prepare PostgreSQL To PostgreSQL Mapping File](#prepare-postgresql-to-postgresql-mapping-file)
  * [PostgreSQL To PostgreSQL Run](#postgresql-to-postgresql-run)
* [PostgreSQL To YDB](#postgresql-to-ydb)
  * [Prepare PostgreSQL To YDB environment](#prepare-postgresql-to-ydb-environment)
  * [Prepare PostgreSQL To YDB Connection Settings](#prepare-postgresql-to-ydb-connection-settings)
  * [Prepare PostgreSQL To YDB Mapping File](#prepare-postgresql-to-ydb-mapping-file)
  * [PostgreSQL To YDB Run](#postgresql-to-ydb-run)
* [For Developers](#for-developers)
  * [init method](#init-method)
  * [Datasource](#datasource)
  * [CqlSession](#cqlsession)
  * [Client](#client)

## Build

Download the source code

```
git clone https://github.com/dimarudik/bublik.git
cd bublik/
```

[Install mvn](https://maven.apache.org/install.html)

Build and package all dependencies

```
mvn clean package -DskipTests
```

## Cassandra To Cassandra
![Cassandra To Cassandra](./bublik-cli/src/test/resources/images/cs2cs.png)

The objective is to migrate data from Cassandra to Cassandra database.
To split data into chunks we use Token Ranges ring of Cassandra. 
Such method helps to minimize the workload on the source database and improves the performance of the data transfer to target database.

### Prepare Cassandra To Cassandra environment

You can run test in TestContainers environment by executing the command below:

```shell
mvn test -Dtest="dev/bublik/cli/cassandra/cassandra/*" -Dsurefire.failIfNoSpecifiedTests=false 
```
Or you can run test case in docker containers manually:

#### Prepare Cassandra Source environment

```shell
docker run --name cassandra1 \
        -h cassandra1 \
        -p 9042:9042 \
        -e CASSANDRA_SNITCH=GossipingPropertyFileSnitch \
        -e JVM_OPTS="-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.initial_token=0" \
        -e HEAP_NEWSIZE=128M \
        -e MAX_HEAP_SIZE=1024M \
        -e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch \
        -e CASSANDRA_DC=datacenter1 \
        -d cassandra
```

To create keyspace and tables run [cqlsh](https://docs.datastax.com/en/dse/6.9/installing/cqlsh.html) script:

```shell
cqlsh -f ./bublik-cli/src/test/resources/cassandra/cassandra/sql/cs-init.cql
```

#### Prepare Cassandra Target environment

```shell
docker run --name cassandra2 \
        -h cassandra2 \
        -p 9043:9042 \
        -e CASSANDRA_SNITCH=GossipingPropertyFileSnitch \
        -e JVM_OPTS="-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.initial_token=0" \
        -e HEAP_NEWSIZE=128M \
        -e MAX_HEAP_SIZE=1024M \
        -e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch \
        -e CASSANDRA_DC=datacenter1 \
        -d cassandra
```

To create keyspace and tables run [cqlsh](https://docs.datastax.com/en/dse/6.9/installing/cqlsh.html) script:

```shell
cqlsh localhost 9043 -f ./bublik-cli/src/test/resources/cassandra/cassandra/sql/cs-init-empty.cql
```

### Prepare Cassandra To Cassandra Connection Settings

Cassandra connection settings `./bublik-cli/src/test/resources/cassandra/cassandra/yaml/cs2cs.yaml`:

```yaml
threadCount: 10

fromProperties:
  class: dev.bublik.cassandra.CassandraStorage
  datacenter: datacenter1
  hosts: localhost
  keyspace: test
  user: test
  password: test
  batchSize: 256
toProperties:
  class: dev.bublik.cassandra.CassandraStorage
  datacenter: datacenter1
  hosts: localhost:9043
  keyspace: test
  user: test
  password: test
  batchSize: 256
```

### Prepare Cassandra To Cassandra Mapping Files

You can run the tool by using json files in folder `./bublik-cli/src/test/resources/cassandra/cassandra/json`.<br>
Moreover you can define the behavior for values of TTL and TIMESTAMP Cassandra internal columns in the mapping file.

> [!IMPORTANT]
> By default, the tool will use the values of TTL and TIMESTAMP from the source values of columns.

Let's consider most interesting cases:

[cs2cs1.json](bublik-cli/src/test/resources/cassandra/cassandra/json/cs2cs1.json)
```json
[
  {
    "fromSchemaName" : "test",
    "fromTableName" : "t1",
    "toSchemaName" : "test",
    "toTableName" : "t1",
    "columnToColumn" : {
      "id"    : "id",
      "uid"   : "uid",
      "v1"    : "v1",
      "v2"    : "v2",
      "v3"    : "v3",
      "v4"    : "v4"
    }
  }
]
```

> [!NOTE]
> In this example, the tool will use the values of TTL and TIMESTAMP from the source values of columns.

[cs2cs2.json](./bublik-cli/src/test/resources/cassandra/cassandra/json/cs2cs2.json):
```json
[
  {
    "fromSchemaName" : "test",
    "fromTableName" : "t2",
    "toSchemaName" : "test",
    "toTableName" : "t2",
    "columnToColumn" : {
      "id"    : "id",
      "uid"   : "uid",
      "v1"    : "v1",
      "v2"    : "v2",
      "v3"    : "v3",
      "v4"    : "v4"
    },
    "withTTL"   : "(int)9999",
    "timestamp" : "(bigint)9999"
  }
]
```

> [!NOTE]
> "withTTL" - defines the TTL value for each row. In the example above TTL value defined as a constant.

> [!NOTE]
> "timestamp" - defines the timestamp value for each row. In the example above TIMESTAMP value defined as a constant.

[cs2cs3.json](bublik-cli/src/test/resources/cassandra/cassandra/json/cs2cs3.json):
```json
[
  {
    "fromSchemaName" : "test",
    "fromTableName" : "t3",
    "toSchemaName" : "test",
    "toTableName" : "t3",
    "columnToColumn" : {
      "id"    : "id",
      "uid"   : "uid",
      "v1"    : "v1",
      "v2"    : "v2",
      "v3"    : "v3",
      "v4"    : "v4"
    },
    "withTTL"   : "NULL"
  }
]
```

> [!NOTE]
> If you need to forget source TTL value during data transfer, you can set "withTTL" as "NULL".

[cs2cs8.json](bublik-cli/src/test/resources/cassandra/cassandra/json/cs2cs8.json):
```json
[
  {
    "fromSchemaName" : "test",
    "fromTableName" : "log_time",
    "toSchemaName" : "test2",
    "toTableName" : "log_time",
    "columnToColumn" : {
      "id"        : "id",
      "uid"       : "uid",
      "log_time"  : "log_time"
    },
    "withTTL" : "cast(60*60*24*365*5 - (toUnixTimestamp(now())/1000 - toUnixTimestamp(log_time)/1000) as int)"
  }
]
```

> [!NOTE]
> "withTTL" - defines the TTL value for each row and can be based on the source column value as shown above.
> In the example above TTL value is calculated as five-year period from the log_time date.

[cs2cs10.json](bublik-cli/src/test/resources/cassandra/cassandra/json/cs2cs10.json):
```json
[
  {
    "fromSchemaName" : "test",
    "fromTableName" : "expression",
    "toSchemaName" : "test",
    "toTableName" : "expression",
    "columnToColumn" : {
      "acc"         : "acc",
      "op_id"       : "op_id",
      "log_date"    : "log_date",
      "v1"          : "v1"
    },
    "expressionToColumn" : {
      "concat(extract_year(log_date), extract_month(log_date)) as year_month" : "year_month"
    }
  }
]
```

> [!NOTE]
> If you need to change structure of the target table, you can use "expressionToColumn" to define new columns.
> In the example above target partition key "year_month" is calculated from the source column "log_date".
> At the target table "year_month" is defined as concatenation of year and month from the source column "log_date",
> instead of just YEAR at the source.

To achieve this you have to create user-defined functions (UDF) in the target Cassandra database, like:

```yaml
CREATE OR REPLACE FUNCTION test.extract_month (input TIMESTAMP)
     RETURNS NULL ON NULL INPUT RETURNS TEXT
     LANGUAGE java AS 'Calendar calendar = Calendar.getInstance(); calendar.setTime(input); int month = calendar.get(Calendar.MONTH) + 1; return month < 10 ? "0" + month : String.valueOf(month);';
CREATE OR REPLACE FUNCTION test.extract_year (input TIMESTAMP)
     RETURNS NULL ON NULL INPUT RETURNS TEXT
     LANGUAGE java AS 'Calendar calendar = Calendar.getInstance(); calendar.setTime(input); int year = calendar.get(Calendar.YEAR); return year + "";';
CREATE OR REPLACE FUNCTION test.concat (s1 TEXT, s2 TEXT)
     RETURNS NULL ON NULL INPUT RETURNS TEXT
     LANGUAGE java AS 'return s1 + s2;';
```

[cs2cs11.json](bublik-cli/src/test/resources/cassandra/cassandra/json/cs2cs11.json):
```json
[
  {
    "fromSchemaName" : "test",
    "fromTableName" : "filter1",
    "toSchemaName" : "test",
    "toTableName" : "filter1",
    "fetchWhereClause" : "id = 1 and uid >= 0",
    "columnToColumn" : {
      "id"    : "id",
      "uid"   : "uid",
      "v1"    : "v1",
      "v2"    : "v2",
      "v3"    : "v3",
      "v4"    : "v4"
    }
  }
]
```

> [!NOTE]
> If you need to filter data from the source table, you can use "fetchWhereClause" to define the filter.

> [!IMPORTANT]
> For filtering use only columns that are part of the primary key.


### Cassandra To Cassandra Run

If you are about do the transfer without downtime, you should parallel the payload to the source and target database. You must save the data to the source database ahead of target database, so that Bublik can not overwrite the data in the target database as per TIMESTAMP value.

```shell
java -jar ./bublik-cli/target/bublik-cli-<version>.jar \
    -k 50000 \
    -c ./bublik-cli/src/test/resources/cassandra/cassandra/yaml/cs2cs.yaml \
    -m ./bublik-cli/src/test/resources/cassandra/cassandra/json/<json-file>.json
```

Chunks will be created automatically with parameter -k at startup

> [!NOTE]
> If the migration was interrupted due to any infrastructure issues you can resume the process without -k parameter.
> In this case unprocessed chunks of data will be transfer

## Cassandra To PostgreSQL
![Cassandra To PostgreSQL](./bublik-cli/src/test/resources/images/cs2pg.png)

The objective is to migrate data from Cassandra to PostgreSQL database.
To split data into chunks we use Token Ranges ring of Cassandra.
Such method helps to minimize the workload on the source database and improves the performance of the data transfer to target database.

### Prepare Cassandra To PostgreSQL environment

You can run test in TestContainers environment by executing the command below:

```shell
mvn test -Dtest="dev/bublik/cli/cassandra/postgresql/*" -Dsurefire.failIfNoSpecifiedTests=false 
```
Or you can run test case in docker containers manually:

#### Prepare Cassandra Source environment

```shell
docker run --name cassandra1 \
        -h cassandra1 \
        -p 9042:9042 \
        -e CASSANDRA_SNITCH=GossipingPropertyFileSnitch \
        -e JVM_OPTS="-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.initial_token=0" \
        -e HEAP_NEWSIZE=128M \
        -e MAX_HEAP_SIZE=1024M \
        -e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch \
        -e CASSANDRA_DC=datacenter1 \
        -d cassandra
```

To create keyspace and tables run [cqlsh](https://docs.datastax.com/en/dse/6.9/installing/cqlsh.html) script:

```shell
cqlsh -f ./bublik-cli/src/test/resources/cassandra/postgresql/sql/cs-init.cql
```

#### Prepare PostgreSQL Target environment

```
docker run --name postgres \
        -h postgres \
        -e POSTGRES_USER=test \
        -e POSTGRES_PASSWORD=test \
        -e POSTGRES_DB=postgres \
        -p 5432:5432 \
        -v ./bublik-cli/src/test/resources/cassandra/postgresql/sql/pg-init.sql:/docker-entrypoint-initdb.d/init.sql \
        -d postgres \
        -c shared_preload_libraries="pg_stat_statements,auto_explain"
```

How to connect to PostgreSQL:

```
psql postgresql://test:test@localhost/postgres
```

### Prepare Cassandra To PostgreSQL Connection Settings

Cassandra and PostgreSQL connection settings `./bublik-cli/src/test/resources/cassandra/postgresql/yaml/cs2pg.yaml`:

```yaml
threadCount: 10

fromProperties:
  class: dev.bublik.cassandra.CassandraStorage
  datacenter: datacenter1
  hosts: localhost
  keyspace: test
  user: cassandra
  password: cassandra
  batchSize: 64
toProperties:
  url: jdbc:postgresql://localhost:5432/postgres?targetServerType=primary
  user: test
  password: test
```

### Prepare Cassandra To PostgreSQL Mapping Files

You can run the tool by using json file in folder `./bublik-cli/src/test/resources/cassandra/postgresql/json/cs2pg.json`

[cs2pg.json](bublik-cli/src/test/resources/cassandra/postgresql/json/cs2pg.json)

```json
[
  {
    "fromSchemaName": "test",
    "fromTableName": "t1",
    "toSchemaName": "public",
    "toTableName": "t1",
    "columnToColumn" : {
      "id" : "id",
      "a" : "a",
      "b" : "b",
      "c" : "c",
      "d" : "d",
      "e" : "e",
      "f" : "f",
      "g" : "g",
      "i" : "i",
      "j" : "j",
      "k" : "k",
      "l" : "l",
      "m" : "m",
      "n" : "n",
      "o" : "o",
      "p" : "p",
      "q" : "q",
      "r" : "r",
      "s" : "s",
      "t" : "t",
      "u" : "u"
    },
    "expressionToColumn" : {
      "test.map_to_json(mmap) as mmap" : "mmap"
    }
  },
  {
    "fromSchemaName": "test",
    "fromTableName": "t1",
    "toSchemaName": "public",
    "toTableName": "t2",
    "columnToColumn" : {
      "id" : "id",
      "a" : "a",
      "b" : "b",
      "c" : "c",
      "d" : "d",
      "e" : "e",
      "f" : "f",
      "g" : "g",
      "i" : "i",
      "j" : "j",
      "k" : "k",
      "l" : "l",
      "m" : "m",
      "n" : "n",
      "o" : "o",
      "p" : "p",
      "q" : "q",
      "r" : "r",
      "s" : "s",
      "t" : "t",
      "u" : "u"
    }
  }
]
```

> [!NOTE]
> To transform map to json you can use UDF function 

```sql
CREATE OR REPLACE FUNCTION test.map_to_json (input map<text, text>)
    RETURNS NULL ON NULL INPUT
    RETURNS text
    LANGUAGE java
    AS $$
        if (input == null || input.isEmpty()) {
            return "{}";
        }
        StringBuilder json = new StringBuilder();
        json.append("{");
        boolean first = true;
        for (java.util.Map.Entry<String, String> entry : input.entrySet()) {
            if (!first) {
                json.append(",");
            }
            String key = entry.getKey().replace("\"", "\\\"");
            String value = entry.getValue().replace("\"", "\\\"");

            json.append("\"").append(key).append("\":\"").append(value).append("\"");
            first = false;
        }
        json.append("}");
        return json.toString();
    $$;
```

### Cassandra To PostgreSQL Run

If you are about do the transfer without downtime, you should parallel the payload to the source and target database. You must save the data to the source database ahead of target database, so that Bublik can not overwrite the data in the target database as per TIMESTAMP value.

```shell
java -jar ./bublik-cli/target/bublik-cli-<version>.jar \
    -k 50000 \
    -c ./bublik-cli/src/test/resources/cassandra/postgresql/yaml/cs2pg.yaml \
    -m ./bublik-cli/src/test/resources/cassandra/postgresql/json/cs2pg.json
```

Chunks will be created automatically with parameter -k at startup

> [!NOTE]
> If the migration was interrupted due to any infrastructure issues you can resume the process without -k parameter.
> In this case unprocessed chunks of data will be transfer

## MS SQL To PostgreSQL
![MS SQL To PostgreSQL](./bublik-cli/src/test/resources/images/mssql2pg.png)

The objective is to migrate data from MS SQL to PostgreSQL database.
To split data into chunks we use Clustering Key of Microsoft SQL Server table.
Such method helps to minimize the workload on the source database and improves the performance of the data transfer to target database.

### Prepare MS SQL To PostgreSQL environment

You can run test in TestContainers environment by executing the command below:

```shell
mvn test -Dtest="dev/bublik/cli/mssql/postgresql/*" -Dsurefire.failIfNoSpecifiedTests=false 
```

Or you can run test case in docker containers manually:

#### Prepare MS SQL environment

```
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=A_Str0ng_Required_Password" --platform linux/amd64 \
   -p 1433:1433 --name mssql \
   -d mcr.microsoft.com/mssql/server:latest
```

>  **WARNING**: All tables participating in test case will be created and fulfilled during docker containers startup

How to connect to MS SQL by sqlcmd:

```
sqlcmd -S localhost -U sa -P "A_Str0ng_Required_Password" -i ./bublik-cli/src/test/resources/mssql/postgresql/sql/mssql-ClusteringKey-GO.sql
```

#### Prepare PostgreSQL environment

```
docker run --name postgres \
        -h postgres \
        -e POSTGRES_USER=test \
        -e POSTGRES_PASSWORD=test \
        -e POSTGRES_DB=postgres \
        -p 5432:5432 \
        -v ./bublik-cli/src/test/resources/mssql/postgresql/sql/pg-init.sql:/docker-entrypoint-initdb.d/init.sql \
        -d postgres \
        -c shared_preload_libraries="pg_stat_statements,auto_explain"
```


How to connect to PostgreSQL:

```
psql postgresql://test:test@localhost/postgres
```

### Prepare MS SQL To PostgreSQL Connection Settings

You can run the tool by using yaml file `./bublik-cli/src/test/resources/mssql/postgresql/yaml/mssql2pg.yaml` with connection settings:

```yaml
threadCount: 4

fromProperties:
  url: jdbc:sqlserver://localhost:1433;databaseName=test;encrypt=true;trustServerCertificate=true;
  user: sa
  password: A_Str0ng_Required_Password
toProperties:
  url: jdbc:postgresql://localhost:5432/postgres?targetServerType=primary
  user: test
  password: test
```

Or you can use environment variables (do not specify -c parameter):

```
export THREAD_COUNT=4
export FROM_URL=jdbc:sqlserver://localhost:1433;databaseName=test;encrypt=true;trustServerCertificate=true;
export FROM_USER=sa
export FROM_PASSWORD=A_Str0ng_Required_Password
export TO_URL=jdbc:postgresql://localhost:5432/postgres
export TO_USER=test
export TO_PASSWORD=test
```

### Prepare MS SQL To PostgreSQL Mapping File

You can run the tool by using json file `./bublik-cli/src/test/resources/mssql/postgresql/json/clusteringKey.json` with mapping settings:

```json
[
  {
    "fromSchemaName": "test",
    "fromTableName": "a",
    "toSchemaName": "public"
  },
  {
    "fromSchemaName": "test",
    "fromTableName": "b",
    "toSchemaName": "public"
  },
  {
    "fromSchemaName": "test",
    "fromTableName": "t1",
    "fromTableAlias": "t",
    "toSchemaName": "public",
    "toTableName": "t1"
  },
  {
    "fromSchemaName": "test",
    "fromTableName": "t2",
    "toSchemaName": "public"
  },
  {
    "fromSchemaName": "test",
    "fromTableName": "t3",
    "toSchemaName": "public"
  },
  {
    "fromSchemaName": "test",
    "fromTableName": "t4",
    "fromTableAlias": "t",
    "toSchemaName": "public"
  }
]
```

or (for example):

```json
[
  {
    "fromSchemaName": "test",
    "fromTableName": "b",
    "fromTableAlias": "s",
    "fromTableAdds": "join test.a on a.id = s.a_id",
    "toSchemaName": "public",
    "toTableName": "a",
    "fetchWhereClause": "a_id > 0",
    "expressionToColumn" : {
      "s.id as id" : "id",
      "a.name as name" : "name"
    }
  }
]
```


### MS SQL To PostgreSQL Run

Halt any changes to the movable tables in the source database (Oracle) and run:

```shell
java -jar ./bublik-cli/target/bublik-cli-<version>.jar \
    -k 50000 \
    -c ./bublik-cli/src/test/resources/mssql/postgresql/yaml/mssql2pg.yaml \
    -m ./bublik-cli/src/test/resources/mssql/postgresql/json/clusteringKey.json
```

Chunks will be created automatically with parameter -k at startup

> [!NOTE]
> If the migration was interrupted due to any infrastructure issues you can resume the process without -k parameter.
> In this case unprocessed chunks of data will be transfer

## Oracle To Cassandra
![Oracle To Cassandra](./bublik-cli/src/test/resources/images/ora2cs.png)

The objective is to migrate data of tables <strong>USERS</strong>, <strong>ITEMS</strong>, <strong>LIKES</strong> from Oracle schema <strong>TEST</strong> to Cassandra database with keyspace <strong>TEST</strong>.
The data transforms to adjust most optimal Cassandra data modeling. 

### Prepare Oracle To Cassandra environment

You can run test in TestContainers environment by executing the command below:

```shell
mvn test -Dtest="dev/bublik/cli/oracle/cassandra/*" -Dsurefire.failIfNoSpecifiedTests=false 
```

Or you can run test case in docker containers manually:

#### Prepare Oracle environment

```shell
docker run --name oracle \
    -p 1521:1521 \
    -e ORACLE_PASSWORD=oracle_4U \
    -v ./bublik-cli/src/test/resources/oracle/cassandra/sql/oracle:/docker-entrypoint-initdb.d \
    -d gvenzl/oracle-free:slim-faststart
```

>  **WARNING**: Tables `USERS`, `ITEMS`, `LIKES` will be created and fulfilled during oracle docker container startup

How to connect to Oracle by [Instant Client](https://www.oracle.com/database/technologies/instant-client.html):

```shell
sqlplus 'test/test@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=freepdb1)))'
```

#### Prepare Cassandra environment

```shell
docker run --name cassandra \
        -h cassandra \
        -p 9042:9042 \
        -e CASSANDRA_SNITCH=GossipingPropertyFileSnitch \
        -e JVM_OPTS="-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.initial_token=0" \
        -e HEAP_NEWSIZE=128M \
        -e MAX_HEAP_SIZE=1024M \
        -e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch \
        -e CASSANDRA_DC=datacenter1 \
        -d cassandra
```

To create keyspace and tables run [cqlsh](https://docs.datastax.com/en/dse/6.9/installing/cqlsh.html) script:

```shell
cqlsh -f ./bublik-cli/src/test/resources/oracle/cassandra/sql/cs-init.cql
```

### Prepare Oracle To Cassandra Connection Settings

You can run the tool by using yaml file `./bublik-cli/src/test/resources/oracle/cassandra/yaml/ora2cs.yaml` with connection settings:


```yaml
threadCount: 10

fromProperties:
  url: jdbc:oracle:thin:@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=freepdb1)))
  user: test
  password: test
toProperties:
  class: dev.bublik.cassandra.CassandraStorage
  datacenter: datacenter1
  hosts: localhost
  keyspace: test
  user: test
  password: test
  batchSize: 128
```

### Prepare Oracle To Cassandra Mapping File

You can run the tool by using json file `./bublik-cli/src/test/resources/oracle/cassandra/json/ora2cs.json` with mapping settings:

```json
[
  {
    "fromSchemaName" : "test",
    "fromTableName" : "likes",
    "fromTableAlias" : "l",
    "fromTableAdds" : "join users u on u.id = l.user_id join items i on i.id = l.item_id",
    "toSchemaName" : "test",
    "toTableName" : "user",
    "fetchWhereClause" : "1 = 1",
    "expressionToColumn" : {
      "l.user_id as user_id"          : "user_id",
      "l.item_id as item_id"          : "item_id",
      "u.user_name as user_name"      : "user_name",
      "u.email as email"              : "email",
      "i.item_name as item_name"      : "item_name",
      "i.description as description"  : "description",
      "l.last_update as last_update"  : "last_update"
    },
    "withTTL" : "86400 * 365 - trunc((EXTRACT (DAY FROM SYSTIMESTAMP) * 24 * 60 * 60) + (EXTRACT (HOUR FROM SYSTIMESTAMP) * 60 * 60) + (EXTRACT (MINUTE FROM SYSTIMESTAMP) * 60) + EXTRACT (SECOND FROM SYSTIMESTAMP))   -   trunc((EXTRACT (DAY FROM l.last_update) * 24 * 60 * 60) + (EXTRACT (HOUR FROM l.last_update) * 60 * 60) + (EXTRACT (MINUTE FROM l.last_update) * 60) + EXTRACT (SECOND FROM l.last_update))",
    "timestamp" : "1762352865634052"
  },
  {
    "fromSchemaName" : "test",
    "fromTableName" : "likes",
    "fromTableAlias" : "l",
    "fromTableAdds" : "join users u on u.id = l.user_id join items i on i.id = l.item_id",
    "toSchemaName" : "test",
    "toTableName" : "item",
    "fetchWhereClause" : "1 = 1",
    "expressionToColumn" : {
      "l.item_id as item_id"          : "item_id",
      "l.user_id as user_id"          : "user_id",
      "i.item_name as item_name"      : "item_name",
      "i.description as description"  : "description",
      "u.user_name as user_name"      : "user_name",
      "u.email as email"              : "email",
      "l.last_update as last_update"  : "last_update"
    },
    "withTTL" : "round(DBMS_RANDOM.VALUE(9000000, 9999999))"
  }
]
```

You can define the behavior for values of TTL and TIMESTAMP Cassandra internal columns in the mapping file.

> [!NOTE] 
> "withTTL" - defines the TTL value for each row and can be based on the source column value as shown above.
> In the example above TTL value is calculated as one-year period from the last update date.

> [!NOTE]
> "timestamp" - defines the timestamp value for each row and can be based on the source column value as shown above.
> In the example above TIMESTAMP value defined as a constant.

### Oracle To Cassandra Run

Halt any changes to the movable tables in the source database (Oracle) and run:

```shell
java -jar ./bublik-cli/target/bublik-cli-<version>.jar \
    -k 50000 \
    -c ./bublik-cli/src/test/resources/oracle/cassandra/yaml/ora2cs.yaml \
    -m ./bublik-cli/src/test/resources/oracle/cassandra/json/ora2cs.json
```

Chunks will be created automatically with parameter -k at startup

> [!NOTE]
> If the migration was interrupted due to any infrastructure issues you can resume the process without -k parameter.
> In this case unprocessed chunks of data will be transfer

## Oracle To PostgreSQL
![Oracle To PostgreSQL](./bublik-cli/src/test/resources/images/ora2pg.png)

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
| number, int, float       | numeric, smallint, bigint, integer, double precision |
| interval year to moth    | interval                                             |
| interval day to second   | interval                                             |

[Java Datatype Mappings](https://docs.oracle.com/en/database/oracle/oracle-database/23/jjdbc/accessing-and-manipulating-Oracle-data.html#GUID-1AF80C90-DFE6-4A3E-A407-52E805726778)

### Prepare Oracle To PostgreSQL environment

You can run test in TestContainers environment by executing the command below:

```shell
mvn test -Dtest="dev/bublik/cli/oracle/postgresql/*" -Dsurefire.failIfNoSpecifiedTests=false 
```

Or you can run test case in docker containers manually:

#### Prepare Oracle environment

```
docker run --name oracle \
    -p 1521:1521 \
    -e ORACLE_PASSWORD=oracle_4U \
    -v ./bublik-cli/src/test/resources/oracle/postgres/sql/oracle:/docker-entrypoint-initdb.d \
    -d gvenzl/oracle-free:slim-faststart
```

>  **WARNING**: All tables participating in test case will be created and fulfilled during docker containers startup

How to connect to Oracle by [Instant Client](https://www.oracle.com/database/technologies/instant-client.html):

```
sqlplus 'test/test@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=freepdb1)))'
```

#### Prepare PostgreSQL environment

```
docker run --name postgres \
        -h postgres \
        -e POSTGRES_USER=test \
        -e POSTGRES_PASSWORD=test \
        -e POSTGRES_DB=postgres \
        -p 5432:5432 \
        -v ./bublik-cli/src/test/resources/oracle/postgres/sql/pg-init-empty.sql:/docker-entrypoint-initdb.d/init.sql \
        -d postgres \
        -c shared_preload_libraries="pg_stat_statements,auto_explain"
```


How to connect to PostgreSQL:

```
psql postgresql://test:test@localhost/postgres
```

### Prepare Oracle To PostgreSQL Connection Settings

You can run the tool by using yaml file `./bublik-cli/src/test/resources/oracle/postgres/yaml/ora2pg.yaml` with connection settings:

```yaml
threadCount: 4

fromProperties:
  url: jdbc:oracle:thin:@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=freepdb1)))
  user: test
  password: test
toProperties:
  url: jdbc:postgresql://localhost:5432/postgres
  user: test
  password: test
```

Or you can use environment variables (do not specify -c parameter):

```
export THREAD_COUNT=4
export FROM_URL=oracle:thin:@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=freepdb1)))
export FROM_USER=test
export FROM_PASSWORD=test
export TO_URL=jdbc:postgresql://localhost:5432/postgres
export TO_USER=test
export TO_PASSWORD=test
```

### Prepare Oracle To PostgreSQL Mapping File

You can run the tool by using json file `./bublik-cli/src/test/resources/oracle/postgres/json/ora2pg.json` with mapping settings:

```json
[
  {
    "fromSchemaName" : "TEST",
    "fromTableName" : "\"Table2\"",
    "toSchemaName" : "PUBLIC",
    "toTableName" : "\"TABLE2\"",
    "fetchHintClause" : "/*+ no_index(TABLE2) */",
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
    "fromTableName" : "INTERVALS",
    "toSchemaName" : "public",
    "toTableName" : "intervals",
    "fetchHintClause" : "/*+ no_index(INTERVALS) */",
    "fetchWhereClause" : "1 = 1",
    "columnToColumn" : {
      "id"            : "id",
      "time_period_1" : "time_period_1",
      "time_period_2" : "time_period_2",
      "time_period_3" : "time_period_3",
      "time_period_4" : "time_period_4"
    }
  },
  {
    "fromSchemaName" : "test",
    "fromTableName" : "table1",
    "fromTableAlias" : "t",
    "fromTableAdds" : "left join test.currencies c on t.currency_id = c.id",
    "fetchHintClause" : "/*+ no_index(T) */",
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
    "fromTableName" : "PARTED",
    "toSchemaName" : "PUBLIC",
    "toTableName" : "PARTED",
    "fetchHintClause" : "/*+ no_index(PARTED) */",
    "fetchWhereClause" : "create_at >= to_date('2022-01-01','YYYY-MM-DD') and create_at <= to_date('2023-12-31','YYYY-MM-DD')",
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

```shell
java -jar ./bublik-cli/target/bublik-cli-<version>.jar \
    -k 50000 \
    -c ./bublik-cli/src/test/resources/oracle/postgres/yaml/ora2pg.yaml \
    -m ./bublik-cli/src/test/resources/oracle/postgres/json/ora2pg.json
```

Chunks will be created automatically with parameter -k at startup

> [!NOTE]
> If the migration was interrupted due to any infrastructure issues you can resume the process without -k parameter.
> In this case unprocessed chunks of data will be transfer 


## Oracle To YDB

![Oracle To PostgreSQL](./bublik-cli/src/test/resources/images/ora2ydb.png)

The objective is to migrate table <strong>to_ydb</strong> to table <strong>to_ydb</strong> from Oracle to YDB.

### Prepare Oracle To YDB environment

You can run test in TestContainers environment by executing the command below:

```shell
mvn test -Dtest="dev/bublik/cli/oracle/ydb/*" -Dsurefire.failIfNoSpecifiedTests=false 
```

Or you can run test case in docker containers manually:

#### Prepare Oracle environment

```
docker run --name oracle \
    -p 1521:1521 \
    -e ORACLE_PASSWORD=oracle_4U \
    -v ./bublik-cli/src/test/resources/oracle/ydb/sql/oracle:/docker-entrypoint-initdb.d \
    -d gvenzl/oracle-free:slim-faststart
```

How to connect to Oracle by [Instant Client](https://www.oracle.com/database/technologies/instant-client.html):

```
sqlplus 'test/test@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=freepdb1)))'
```

#### Prepare YDB environment

Do the next steps:

```shell
docker run -d --rm --name ydb -h localhost \
  --platform linux/amd64 \
  -p 2135:2135 -p 2136:2136 -p 8765:8765 -p 9092:9092 \
  -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
  -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
  -e YDB_KAFKA_PROXY_PORT=9092 \
  ydbplatform/local-ydb:latest
```

To create tables run [ydb](https://ydb.tech/docs/en/reference/ydb-cli/install) script:

```shell
ydb -e grpc://localhost:2136 -d /local yql -s 'create table to_ydb (id Uint32, name String, primary key (id))'
```

<ul><li>How to connect to YDB</li></ul>

```
ydb -e grpc://localhost:2136 -d /local
```

### Prepare Oracle To YDB Connection Settings

You can run the tool by using yaml file `./bublik-cli/src/test/resources/oracle/ydb/yaml/ora2ydb.yaml` with connection settings:

```yaml
threadCount: 4

fromProperties:
  url: jdbc:oracle:thin:@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=freepdb1)))
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
export FROM_URL=oracle:thin:@(description=(address=(host=localhost)(protocol=tcp)(port=1521))(connect_data=(service_name=freepdb1)))
export FROM_USER=test
export FROM_PASSWORD=test
export TO_URL=jdbc:ydb:grpc://localhost:2136/local
export TO_USER=""
export TO_PASSWORD=""
```

### Prepare Oracle To YDB Mapping File

You can run the tool by using json file `./bublik-cli/src/test/resources/oracle/ydb/json/ora2ydb.json` with mapping settings:

```json
[
  {
    "fromSchemaName": "test",
    "fromTableName": "to_ydb",
    "toSchemaName": "",
    "toTableName": "to_ydb",
    "columnToColumn" : {
      "id": "id",
      "name": "name"
    }
  }
]
```

### Oracle To YDB Run

Halt any changes to the movable tables in the source database (Oracle) and run:

```shell
java -jar ./bublik-cli/target/bublik-cli-<version>.jar \
    -k 50000 \
    -c ./bublik-cli/src/test/resources/oracle/ydb/yaml/ora2ydb.yaml \
    -m ./bublik-cli/src/test/resources/oracle/ydb/json/ora2ydb.json
```

Chunks will be created automatically with parameter -k at startup

> [!NOTE]
> If the migration was interrupted due to any infrastructure issues you can resume the process without -k parameter.
> In this case unprocessed chunks of data will be transfer



## PostgreSQL To Cassandra
![Oracle To Cassandra](./bublik-cli/src/test/resources/images/pg2cs.png)

The objective is to migrate data of tables <strong>USERS</strong>, <strong>ITEMS</strong>, <strong>LIKES</strong> from PostgreSQL schema <strong>TEST</strong> to a Cassandra database keyspace <strong>TEST</strong>.
The data transforms to adjust most optimal Cassandra data modeling.

### Prepare PostgreSQL To Cassandra environment

You can run test in TestContainers environment by executing the command below:

```shell
mvn test -Dtest="dev/bublik/cli/postgresql/cassandra/*" -Dsurefire.failIfNoSpecifiedTests=false 
```

Or you can run test case in docker containers manually:

#### Prepare PostgreSQL environment

```shell
docker run --name postgres \
        -h postgres \
        -e POSTGRES_USER=test \
        -e POSTGRES_PASSWORD=test \
        -e POSTGRES_DB=postgres \
        -p 5432:5432 \
        -v ./bublik-cli/src/test/resources/postgresql/cassandra/sql/pg-init.sql:/docker-entrypoint-initdb.d/init.sql \
        -d postgres \
        -c shared_preload_libraries="pg_stat_statements,auto_explain"
```

>  **WARNING**: Tables `USERS`, `ITEMS`, `LIKES` will be created and fulfilled during oracle docker container startup


#### Prepare Cassandra environment

```shell
docker run --name cassandra \
        -h cassandra \
        -p 9042:9042 \
        -e CASSANDRA_SNITCH=GossipingPropertyFileSnitch \
        -e JVM_OPTS="-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.initial_token=0" \
        -e HEAP_NEWSIZE=128M \
        -e MAX_HEAP_SIZE=1024M \
        -e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch \
        -e CASSANDRA_DC=datacenter1 \
        -d cassandra
```

To create keyspace and tables run [cqlsh](https://docs.datastax.com/en/dse/6.9/installing/cqlsh.html) script:

```shell
cqlsh -f ./bublik-cli/src/test/resources/postgresql/cassandra/sql/cs-init.cql
```

### Prepare PostgreSQL To Cassandra Connection Settings

You can run the tool by using yaml file `./bublik-cli/src/test/resources/postgresql/cassandra/yaml/pg2cs.yaml` with connection settings:


```yaml
threadCount: 10

fromProperties:
  url: jdbc:postgresql://localhost:5432/postgres?targetServerType=primary&options=-c%20enable_indexscan=off%20-c%20enable_indexonlyscan=off%20-c%20enable_bitmapscan=off
  user: test
  password: test
toProperties:
  class: dev.bublik.cassandra.CassandraStorage
  datacenter: datacenter1
  hosts: localhost
  keyspace: test
  user: test
  password: test
  batchSize: 128
```

### Prepare PostgreSQL To Cassandra Mapping File

You can run the tool by using json file `./bublik-cli/src/test/resources/postgresql/cassandra/json/pg2cs.json` with mapping settings:

```json
[
  {
    "fromSchemaName" : "public",
    "fromTableName" : "likes",
    "fromTableAlias" : "l",
    "fromTableAdds" : "left join users u on u.id = l.user_id left join items i on i.id = l.item_id",
    "toSchemaName" : "test",
    "toTableName" : "user",
    "fetchWhereClause" : "1 = 1",
    "expressionToColumn" : {
      "l.user_id as user_id"          : "user_id",
      "l.item_id as item_id"          : "item_id",
      "u.user_name as user_name"      : "user_name",
      "u.email as email"              : "email",
      "i.item_name as item_name"      : "item_name",
      "i.description as description"  : "description",
      "l.last_update as last_update"  : "last_update"
    },
    "withTTL" : "86400 * 365 - ((extract(epoch from now()))::int - (extract(epoch from l.last_update))::int)"
  },
  {
    "fromSchemaName" : "public",
    "fromTableName" : "likes",
    "fromTableAlias" : "l",
    "fromTableAdds" : "left join users u on u.id = l.user_id left join items i on i.id = l.item_id",
    "toSchemaName" : "test",
    "toTableName" : "item",
    "fetchWhereClause" : "1 = 1",
    "expressionToColumn" : {
      "l.item_id as item_id"          : "item_id",
      "l.user_id as user_id"          : "user_id",
      "i.item_name as item_name"      : "item_name",
      "i.description as description"  : "description",
      "u.user_name as user_name"      : "user_name",
      "u.email as email"              : "email",
      "l.last_update as last_update"  : "last_update"
    },
    "withTTL" : "floor(random() * (9999999 - 9000000 + 1) + 9000000)::int"
  },
  {
    "fromSchemaName" : "public",
    "fromTableName" : "likes",
    "fromTableAlias" : "l",
    "fromTableAdds" : "left join users u on u.id = l.user_id left join items i on i.id = l.item_id",
    "toSchemaName" : "test",
    "toTableName" : "user2",
    "fetchWhereClause" : "1 = 1",
    "expressionToColumn" : {
      "l.user_id as user_id"          : "user_id",
      "l.item_id as item_id"          : "item_id",
      "u.user_name as user_name"      : "user_name",
      "u.email as email"              : "email",
      "i.item_name as item_name"      : "item_name",
      "i.description as description"  : "description",
      "l.last_update as last_update"  : "last_update"
    },
    "withTTL" : "NULL",
    "timestamp" : "(extract(epoch from l.last_update) * 1000000)::bigint"
  }
]
```

You can define the behavior for values of TTL and TIMESTAMP Cassandra internal columns in the mapping file.

> [!NOTE]
> "withTTL" - defines the TTL value for each row and can be based on the source column value as shown above.
> In the example above TTL value is calculated as one-year period from the last update date.

> [!NOTE]
> "timestamp" - defines the timestamp value for each row and can be based on the source column value as shown above.

### PostgreSQL To Cassandra Run

Halt any changes to the movable tables in the source database (Oracle) and run:

```shell
java -jar ./bublik-cli/target/bublik-cli-<version>.jar \
    -k 50000 \
    -c ./bublik-cli/src/test/resources/postgresql/cassandra/yaml/pg2cs.yaml \
    -m ./bublik-cli/src/test/resources/postgresql/cassandra/json/pg2cs.json
```

Chunks will be created automatically with parameter -k at startup

> [!NOTE]
> If the migration was interrupted due to any infrastructure issues you can resume the process without -k parameter.
> In this case unprocessed chunks of data will be transfer


## PostgreSQL To PostgreSQL
![PostgreSQL To PostgreSQL](./bublik-cli/src/test/resources/images/pg2pg.png)

The objective is to migrate partitions of table from one PostgreSQL database to another. To simplify test case we're using same database

> [!NOTE]
> Bublik uses Tid Range Scan to retrieve data, however this access method has been implemented in PostgreSQL 14.0 and later.
> Therefore please use PostgreSQL >= 14.0 at source side

[E.18.3.1.4. Optimizer](https://www.postgresql.org/docs/14/release-14.html#id-1.11.6.23.5)


You can run test in TestContainers environment by executing the command below:

```shell
mvn test -Dtest="dev/bublik/cli/postgresql/postgresql/*" -Dsurefire.failIfNoSpecifiedTests=false
```

Or you can run test case in docker containers manually:

### Prepare PostgreSQL To PostgreSQL environment

```shell
docker run --name postgres \
        -h postgres \
        -e POSTGRES_USER=test \
        -e POSTGRES_PASSWORD=test \
        -e POSTGRES_DB=postgres \
        -p 5432:5432 \
        -v ./bublik-cli/src/test/resources/postgresql/postgresql/sql/pg-init.sql:/docker-entrypoint-initdb.d/init.sql \
        -v ./sql/bublik.png:/var/lib/postgresql/bublik.png \
        -d postgres \
        -c shared_preload_libraries="pg_stat_statements,auto_explain"
```

### Prepare PostgreSQL To PostgreSQL Connection Settings

You can run the tool by using yaml file `./bublik-cli/src/test/resources/postgresql/postgresql/yaml/pg2pg.yaml` with connection settings:

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

You can run the tool by using json file `./bublik-cli/src/test/resources/postgresql/postgresql/json/pg2pg.json` with mapping settings:

```json
[
  {
    "fromSchemaName": "public",
    "fromTableName": "p_src_202510",
    "toSchemaName": "public",
    "toTableName": "p_trg_202510",
    "fetchWhereClause": "0 = 0"
  },
  {
    "fromSchemaName": "public",
    "fromTableName": "p_src_202511",
    "toSchemaName": "public",
    "toTableName": "p_trg_202511",
    "fetchWhereClause": "1 = 1",
    "columnToColumn": {
      "id" : "id",
      "created" : "created",
      "name" : "name",
      "amount" : "amount"
    },
    "expressionToColumn" : {
      "0 as shard_key" : "shard_key"
    }
  },
  {
    "fromSchemaName": "public",
    "fromTableName": "p_src_202512",
    "toSchemaName": "public",
    "toTableName": "p_trg_202512",
    "fetchWhereClause": "1 = 1",
    "expressionToColumn" : {
      "id" : "id",
      "created" : "created",
      "name" : "name",
      "amount" : "amount",
      "0 as shard_key" : "shard_key"
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

```shell
java -jar ./bublik-cli/target/bublik-cli-<version>.jar \
    -k 50000 \
    -c ./bublik-cli/src/test/resources/postgresql/postgresql/yaml/pg2pg.yaml \
    -m ./bublik-cli/src/test/resources/postgresql/postgresql/json/pg2pg.json
```

Chunks will be created automatically with parameter -k at startup

> [!NOTE]
> If the migration was interrupted due to any infrastructure issues you can resume the process without -k parameter.
> In this case unprocessed chunks of data will be transfer

> [!IMPORTANT]
> Due to chunk creation based on statistics of the table
> please check that ANALYZE is performed on regular basis


## PostgreSQL To YDB
![PostgreSQL To YDB](./bublik-cli/src/test/resources/images/pg2ydb.png)

The objective is to migrate table <strong>likes</strong> to table <strong>likes_all</strong> from PostgreSQL to YDB with enrichment of data from other tables.

> [!NOTE]
> Bublik uses Tid Range Scan to retrieve data, however this access method has been implemented in PostgreSQL 14.0 and later.
> Therefore please use PostgreSQL >= 14.0 at source side

[E.18.3.1.4. Optimizer](https://www.postgresql.org/docs/14/release-14.html#id-1.11.6.23.5)

You can run test in TestContainers environment by executing the command below:

```shell
mvn test -Dtest="dev/bublik/cli/postgresql/ydb/*" -Dsurefire.failIfNoSpecifiedTests=false 
```

Or you can run test case in docker containers manually:

### Prepare PostgreSQL To YDB environment

```shell
docker run --name postgres \
        -h postgres \
        -e POSTGRES_USER=test \
        -e POSTGRES_PASSWORD=test \
        -e POSTGRES_DB=postgres \
        -p 5432:5432 \
        -v ./bublik-cli/src/test/resources/postgresql/ydb/sql/pg-init.sql:/docker-entrypoint-initdb.d/init.sql \
        -d postgres \
        -c shared_preload_libraries="pg_stat_statements,auto_explain"
```


[YDB Quick Start](https://ydb.tech/docs/en/quickstart?tabs=defaultTabsGroup-3dol9c63_docker%2520x86_64)

Do the next steps to prepare YDB environment:

```shell
docker run -d --rm --name ydb -h localhost \
  --platform linux/amd64 \
  -p 2135:2135 -p 2136:2136 -p 8765:8765 -p 9092:9092 \
  -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
  -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
  -e YDB_KAFKA_PROXY_PORT=9092 \
  ydbplatform/local-ydb:latest
```

To create tables run [ydb](https://ydb.tech/docs/en/reference/ydb-cli/install) script:

```shell
ydb -e grpc://localhost:2136 -d /local yql -s 'create table to_ydb (id Uint32, name String, primary key (id))'
```

<ul><li>How to connect to YDB</li></ul>

```
ydb -e grpc://localhost:2136 -d /local
```

### Prepare PostgreSQL To YDB Connection Settings

You can run the tool by using yaml file `./bublik-cli/src/test/resources/postgresql/ydb/yaml/pg2ydb.yaml` with connection settings:

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

```shell
export THREAD_COUNT=4
export FROM_URL=jdbc:postgresql://localhost:5432/postgres?options=-c%20enable_indexscan=off%20-c%20enable_indexonlyscan=off%20-c%20enable_bitmapscan=off
export FROM_USER=test
export FROM_PASSWORD=test
export TO_URL=jdbc:ydb:grpc://localhost:2136/local
export TO_USER=""
export TO_PASSWORD="
```

### Prepare PostgreSQL To YDB Mapping File

You can run the tool by using json file `./bublik-cli/src/test/resources/postgresql/ydb/json/pg2ydb.json` with mapping settings:

```json
[
  {
    "fromSchemaName": "public",
    "fromTableName": "to_ydb",
    "toSchemaName": "",
    "toTableName": "to_ydb",
    "columnToColumn" : {
      "id": "id",
      "name": "name"
    }
  }
]
```


### PostgreSQL To YDB Run

Halt any changes to the movable tables in the source database and run:

```shell
java -jar ./bublik-cli/target/bublik-cli-<version>.jar \
    -k 50000 \
    -c ./bublik-cli/src/test/resources/postgresql/ydb/yaml/pg2ydb.yaml \
    -m ./bublik-cli/src/test/resources/postgresql/ydb/json/pg2ydb.json
```

Chunks will be created automatically with parameter -k at startup

> [!NOTE]
> If the migration was interrupted due to any infrastructure issues you can resume the process without -k parameter.
> In this case unprocessed chunks of data will be transfer

> [!IMPORTANT]
> Due to chunk creation based on statistics of the table
> please check that ANALYZE is performed on regular basis


## For Developers

You can use Bublik's libs in your own projects by adding the following dependency to your pom.xml:

```xml
    <dependency>
        <groupId>dev.bublik</groupId>
        <artifactId>bublik-cassandra</artifactId>
        <version>${version}</version>
    </dependency>
    <dependency>
      <groupId>dev.bublik</groupId>
      <artifactId>bublik-clickhouse</artifactId>
      <version>${version}</version>
      </dependency>
    <dependency>
        <groupId>dev.bublik</groupId>
        <artifactId>bublik-mssql</artifactId>
        <version>${version}</version>
    </dependency>
    <dependency>
        <groupId>dev.bublik</groupId>
        <artifactId>bublik-oracle</artifactId>
        <version>${version}</version>
    </dependency>
    <dependency>
        <groupId>dev.bublik</groupId>
        <artifactId>bublik-postgres</artifactId>
        <version>${version}</version>
    </dependency>
    <dependency>
        <groupId>dev.bublik</groupId>
        <artifactId>bublik-ydb</artifactId>
        <version>${version}</version>
    </dependency>
```

The list of needed dependencies is based on types of source and target databases.<br>
If you want to migrate data from PostgreSQL to PostgreSQL you need only dependency:
```xml
    <dependency>
        <groupId>dev.bublik</groupId>
        <artifactId>bublik-postgres</artifactId>
        <version>${version}</version>
    </dependency>
```

If your source and target have different types you need to add two dependencies.<br>
For example, if you want to migrate data from PostgreSQL to Cassandra you need to add the following two dependencies:
```xml
    <dependency>
        <groupId>dev.bublik</groupId>
        <artifactId>bublik-postgres</artifactId>
        <version>${version}</version>
    </dependency>
    <dependency>
        <groupId>dev.bublik</groupId>
        <artifactId>bublik-cassandra</artifactId>
        <version>${version}</version>
    </dependency>
```

### init method

The easiest way to start migration is to use init method.

Create ConnectionProperty (example from TestContainers):
```java
ConnectionProperty getConnectionProperty() {
    Map<String, String> fromProps = new HashMap<>();
    fromProps.put("url", postgres.getJdbcUrl());
    fromProps.put("user", postgres.getUsername());
    fromProps.put("password", postgres.getPassword());

    Map<String, String> toProps = new HashMap<>();
    toProps.put("url", postgres.getJdbcUrl());
    toProps.put("user", postgres.getUsername());
    toProps.put("password", postgres.getPassword());

    return new ConnectionProperty(
            4, // number of threads
            fromProps,
            toProps,
            new HashMap<>(),
            new HashMap<>()
    );
}
```

Create Config:
```java
List<Config> configs = new ArrayList<>();
Config tableConfig = new Config(
        "public",
        "source_users",
        "public",
        "target_users"
);
configs.add(tableConfig);
```

Create chunk and outbox tables (the tables will be created at source and target side):
```java
Table chunkTable = new PseudoTable("public", "chunk");
Table outboxTable = new PseudoTable("public", "outbox");
```

Run the migration:
```java
StorageService.init(connectionProperty, configs, 1000, chunkTable, outboxTable);
```

Full example [`InitPostgresMigrationTest.java`](bublik-postgres/src/test/java/dev/bublik/postgres/InitPostgresMigrationTest.java)


### Datasource

You can provide your own datasource to the migration.<br>
The usage of datasource applicable only for jdbc-like storages.

Create two datasources for source and target:

```java
HikariConfig sourceConfig = new HikariConfig();
sourceConfig.setJdbcUrl(oracle.getJdbcUrl());
sourceConfig.setUsername(oracle.getUsername());
sourceConfig.setPassword(oracle.getPassword());
sourceConfig.setMaximumPoolSize(5);
HikariDataSource sourceDataSource = new HikariDataSource(sourceConfig);

HikariConfig targetConfig = new HikariConfig();
targetConfig.setJdbcUrl(postgres.getJdbcUrl());
targetConfig.setUsername(postgres.getUsername());
targetConfig.setPassword(postgres.getPassword());
targetConfig.setMaximumPoolSize(5);
HikariDataSource targetDataSource = new HikariDataSource(targetConfig);
```

Create chunk and outbox tables (the tables will be created at source and target side):
```java
Table sourceChunkTable = new PseudoTable(null, "foo");
Table targetOutboxTable = new PseudoTable("public", "bublik");
```

Create two storages (source and target):
```java
Storage sourceStorage = new OracleStorage(sourceDataSource, sourceChunkTable);
Storage targetStorage = new PostgresStorage(targetDataSource, targetOutboxTable);
```

Create Config:
```java
List<Config> configs = new ArrayList<>();
Config tableConfig = new Config(
        oracle.getUsername().toUpperCase(),
        "SOURCE_USERS",
        "public",
        "target_users"
);
configs.add(tableConfig);
```
Run the migration:
```java
sourceStorage.start(targetStorage, configs, 1000);
```

Full example: [`ConstructorPostgresMigrationTest.java`](bublik-postgres/src/test/java/dev/bublik/postgres/ConstructorPostgresMigrationTest.java)

### CqlSession

For Cassandra you can provide your own CqlSession to the migration.

Create two CqlSession objects or CqlSession and Datasource:
```java
DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
        .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, expectedPoolSize)
        .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, expectedPoolSize)
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
        .build();

InetSocketAddress contactPoint = new InetSocketAddress(
        cassandraContainer.getHost(),
        cassandraContainer.getMappedPort(9042)
);

CqlSession sourceSession = CqlSession.builder()
        .addContactPoint(contactPoint)
        .withConfigLoader(configLoader)
        .withAuthCredentials(cassandraContainer.getUsername(), cassandraContainer.getPassword())
        .withLocalDatacenter(cassandraContainer.getLocalDatacenter())
        .build();

CqlSession targetSession = CqlSession.builder()
                .addContactPoint(contactPoint)
                .withConfigLoader(configLoader)
                .withAuthCredentials(cassandraContainer.getUsername(), cassandraContainer.getPassword())
        .withLocalDatacenter(cassandraContainer.getLocalDatacenter())
        .build();
```

Create chunk and outbox tables (the tables will be created at source and target side):
```java
Table sourceOutboxTable = new PseudoTable(sourceKeyspace, "bublik");
Table targetOutboxTable = new PseudoTable(targetKeyspace, "bublik");
```

Create two storages (source and target):
```java
Storage sourceStorage = new CassandraStorage(sourceSession, batchSize, sourceOutboxTable);
Storage targetStorage = new CassandraStorage(targetSession, batchSize, targetOutboxTable);
```

Create Config:
```java
        List<Config> configs = new ArrayList<>();
        Config tableConfig = new Config(
                "bublik_source",
                "source_users",
                "bublik_target",
                "target_users"
        );
        configs.add(tableConfig);
```

Run the migration:
```java
sourceStorage.start(targetStorage, configs, 1000);
```

Full example: [`CassandraMigrationTest.java`](bublik-cassandra/src/test/java/dev/bublik/cassandra/CassandraMigrationTest.java)

### Client

For Clickhouse you can provide your own Client to the migration.

Create Datasource and Client:
```java
HikariConfig sourceConfig = new HikariConfig();
sourceConfig.setJdbcUrl(oracle.getJdbcUrl());
sourceConfig.setUsername(oracle.getUsername());
sourceConfig.setPassword(oracle.getPassword());
sourceConfig.setMaximumPoolSize(5);
HikariDataSource sourceDataSource = new HikariDataSource(sourceConfig);

int expectedClickhousePoolSize = 5;
String clickhouseHttpUrl = "http://" + clickhouse.getHost() + ":" + clickhouse.getMappedPort(8123);
Client clickhouseClient = new Client.Builder()
        .addEndpoint(clickhouseHttpUrl)
        .setDefaultDatabase("default")
        .setUsername(clickhouse.getUsername())
        .setPassword(clickhouse.getPassword())
        .setMaxConnections(expectedClickhousePoolSize)
        .setConnectTimeout(10, ChronoUnit.SECONDS)
        .setSocketTimeout(5, ChronoUnit.MINUTES)
        .build();
```

Create chunk and outbox tables (the tables will be created at source and target side):
```java
Table sourceOutboxTable = new PseudoTable("public", "source_outbox");
Table targetOutboxTable = new PseudoTable(null, "target_outbox");
```

Create two storages (source and target):
```java
Storage sourceStorage = new OracleStorage(sourceDataSource, sourceOutboxTable);
Storage targetStorage = new ClickHouseStorage(clickhouseClient, targetOutboxTable);
```

Create Config:
```java
List<Config> configs = new ArrayList<>();
Config tableConfig = new Config(
        oracle.getUsername().toUpperCase(),
        "SOURCE_USERS",
        "default",
        "target_users"
);
configs.add(tableConfig);
```
Run the migration:
```java
sourceStorage.start(targetStorage, configs, 1000);
```

Full example: [`ClickHouseMigrationTest.java`](bublik-oracle/src/test/java/dev/bublik/oracle/ClickHouseMigrationTest.java)