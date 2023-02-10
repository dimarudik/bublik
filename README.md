**Hints**

Start / Stop / List / Exec for Postgres in docker-compose

```
docker-compose up -d
docker-compose down
docker-compose ps
docker exec -it shard00 /bin/bash
```

Connect to Postgres remotely

```
psql -d accounts_hidden_dev -U accounts_hidden_dev -W -h localhost
```

Delete untagged images / unused volumes

```
docker rmi $(docker images -f "dangling=true" -q)
docker volume prune
```

---
PSQL Commands

```
\l+   # show databases
\dt+  # show tables
```

---
MVN build
```
mvn package -DskipTests
```

---
RUN
```
/opt/oracle/jdk-14.0.1/bin/java -jar ora2pgsql-1.0-SNAPSHOT.jar
```