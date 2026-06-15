CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

CREATE TABLE test.ttl_check (
    id bigint,
    offer_id bigint,
    flags tinyint,
    primary key (id)
);