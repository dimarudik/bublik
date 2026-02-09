CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create table test.t_blob (
    key int,
    value blob,
    v blob,
    primary key (key, value));
