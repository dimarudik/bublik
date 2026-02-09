CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
CREATE KEYSPACE IF NOT EXISTS test2 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create table test.t_blob (
    key int,
    value blob,
    v blob,
    primary key (key, value));

insert into test.t_blob (key, value, v) values (1, 0x0102030405060708090a0b0c0d0e0f101112131415161718, 0x0102030405060708090a0b0c0d0e0f101112131415161718);

create table test2.t_blob (
    key int,
    value blob,
    v blob,
    primary key (key, value));
