--CREATE ROLE test WITH PASSWORD = 'test' AND SUPERUSER = true AND LOGIN = true;
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create table test.t (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    primary key (id, uid));

insert into test.t (id, uid, v1, v2, v3) values (1, 0, 1, 1, 1);
insert into test.t (id, uid, v1, v2, v3) values (1, 1, 1, 1, 1);
insert into test.t (id, uid, v1, v2, v3) values (1, 2, 1, 1, 1);
insert into test.t (id, uid, v1, v2, v3) values (1, 3, 1, 1, 1);
insert into test.t (id, uid, v1, v2, v3) values (1, 4, 1, 1, 1);
insert into test.t (id, uid, v1, v2, v3) values (1, 5, 1, 1, 1);
insert into test.t (id, uid, v1, v2, v3) values (1, 6, 1, 1, 1);
insert into test.t (id, uid, v1, v2, v3) values (1, 7, 1, 1, 1);
insert into test.t (id, uid, v1, v2, v3) values (1, 8, 1, 1, 1);
insert into test.t (id, uid, v1, v2, v3) values (1, 9, 1, 1, 1);
