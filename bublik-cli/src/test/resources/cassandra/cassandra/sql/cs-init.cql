--CREATE ROLE test WITH PASSWORD = 'test' AND SUPERUSER = true AND LOGIN = true;
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create table test.t (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    primary key (id, uid));

insert into test.t (id, uid, v1, v2, v3) values (1, 0, 11, 21, 30);
insert into test.t (id, uid, v1, v2, v3) values (1, 1, 11, 21, 31);
insert into test.t (id, uid, v1, v2, v3) values (1, 2, 11, 21, 32);
insert into test.t (id, uid, v1, v2, v3) values (1, 3, 11, 21, 33);
insert into test.t (id, uid, v1, v2, v3) values (1, 4, 11, 21, 34);
insert into test.t (id, uid, v1, v2, v3) values (2, 0, 11, 21, 35);
insert into test.t (id, uid, v1, v2, v3) values (2, 1, 11, 21, 36);
insert into test.t (id, uid, v1, v2, v3) values (2, 2, 11, 21, 37);
insert into test.t (id, uid, v1, v2, v3) values (2, 4, 11, 21, 38);
insert into test.t (id, uid, v1, v2, v3) values (2, 5, 11, 21, 39);

create table test.emty (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    primary key (id, uid));
