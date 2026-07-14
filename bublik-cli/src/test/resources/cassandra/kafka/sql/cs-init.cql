CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create table test.t4 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));

create table test.t5 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));

insert into test.t4 (id, uid, v1, v2, v3, v4) values (1, 2, 3,4,5,'a');
insert into test.t5 (id, uid, v1, v2, v3, v4) values (6, 7, 8,9,10,'6');
