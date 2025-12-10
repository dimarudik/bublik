CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create table test.t1 (
    id int,
    v1 int,
    primary key (id));
insert into test.t1 (id, v1) values (1, 1);
insert into test.t1 (id, v1) values (2, 2);
