--CREATE ROLE test WITH PASSWORD = 'test' AND SUPERUSER = true AND LOGIN = true;
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'DC2': '3'}  AND durable_writes = true;

create table test.t1 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));
