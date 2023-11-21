
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