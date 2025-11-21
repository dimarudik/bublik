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
insert into test.t1 (id, uid, v1, v2, v3, v4) values (1, 0, 11, 21, 30, 'a');

insert into test.t1 (id, uid, v1            ) values (1, 1, 11             ) using ttl 1000;
insert into test.t1 (id, uid,     v2, v3, v4) values (1, 1,     21, 31, 'a') using ttl 2000;

insert into test.t1 (id, uid, v1, v2, v3, v4) values (1, 2, 11, 21, 32, 'a');

insert into test.t1 (id, uid, v1, v2, v3, v4) values (2, 1, 11, 21, 36, 'a') using timestamp 1000000000000001;

insert into test.t1 (id, uid, v1, v2, v3, v4) values (2, 2, 11, 21, 37, 'a') using timestamp 1000000000000002;

insert into test.t1 (id, uid, v1            ) values (2, 0, 11             ) using ttl 3000 and timestamp 1000000000000003;
insert into test.t1 (id, uid,     v2        ) values (2, 0,     21         ) using ttl 4000 and timestamp 1000000000000004;
insert into test.t1 (id, uid,             v4) values (2, 0,             'a') using ttl 5000 and timestamp 1000000000000005;
insert into test.t1 (id, uid,         v3    ) values (2, 0,         35     ) using ttl 6000 and timestamp 1000000000000006;

create table test.t2 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));
insert into test.t2 (id, uid, v1, v2, v3, v4) values (1, 0, 11, 21, 30, 'a');
insert into test.t2 (id, uid, v1, v2, v3, v4) values (1, 1, 11, 21, 31, 'a') using ttl 1000;
insert into test.t2 (id, uid, v1, v2, v3, v4) values (1, 2, 11, 21, 32, 'a') using ttl 1000 and timestamp 10000000000;
insert into test.t2 (id, uid, v1, v2, v3, v4) values (2, 0, 11, 21, 35, 'a');
insert into test.t2 (id, uid, v1, v2, v3, v4) values (2, 1, 11, 21, 36, 'a');
insert into test.t2 (id, uid, v1, v2, v3, v4) values (2, 2, 11, 21, 37, 'a');

create table test.t3 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));
insert into test.t3 (id, uid, v1, v2, v3, v4) values (1, 0, 11, 21, 30, 'a');
insert into test.t3 (id, uid, v1, v2, v3, v4) values (1, 1, 11, 21, 31, 'a') using ttl 1000;
insert into test.t3 (id, uid, v1, v2, v3, v4) values (1, 2, 11, 21, 32, 'a') using ttl 1000 and timestamp 10000000000;
insert into test.t3 (id, uid, v1, v2, v3, v4) values (2, 0, 11, 21, 35, 'a');
insert into test.t3 (id, uid, v1, v2, v3, v4) values (2, 1, 11, 21, 36, 'a');
insert into test.t3 (id, uid, v1, v2, v3, v4) values (2, 2, 11, 21, 37, 'a');
