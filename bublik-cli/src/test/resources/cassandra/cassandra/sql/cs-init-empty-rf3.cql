--CREATE USER test WITH PASSWORD 'test' SUPERUSER;
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'DC2': '3'}  AND durable_writes = true;
--GRANT ALL PERMISSIONS ON KEYSPACE TEST to TEST;

create table test.t4 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));
