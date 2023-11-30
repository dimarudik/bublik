```text
Is a tool to copy data from Oracle to Postgresql
The fastest way to obtain data from Oracle is to get it by ROWID (use dbms_parallel_execute to prepare chunks)
The fastest way to put data to Postgresql is to insert it by using COPY (except LOB's)
```

Build
```
mvn clean package -DskipTests
```

Run
```
java -jar -Xmx4g ora2pgsql-1.1-SNAPSHOT.jar
```

sqlstatement.json
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