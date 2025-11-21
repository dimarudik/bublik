--CREATE ROLE test WITH PASSWORD = 'test' AND SUPERUSER = true AND LOGIN = true;
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create table test.t1 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));

create table test.t2 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));

create table test.t3 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));
