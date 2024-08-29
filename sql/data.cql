ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'};
CREATE ROLE test WITH PASSWORD = 'test' AND SUPERUSER = true AND LOGIN = true;
CREATE KEYSPACE IF NOT EXISTS store WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : '3' };

CREATE TABLE IF NOT EXISTS store.shopping_cart (
userid text PRIMARY KEY,
item_count int,
last_update_timestamp timestamp
);

BEGIN BATCH
INSERT INTO store.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('9876', 2, toTimeStamp(now()));
INSERT INTO store.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('1234', 5, toTimeStamp(now()));
INSERT INTO store.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('27', 27, toTimeStamp(now()));
APPLY BATCH;

CREATE TABLE IF NOT EXISTS store.ckey (
name text,
id int,
item_count int,
last_update_timestamp timestamp,
primary key ((name, id))
);
insert into store.ckey (name, id) VALUES ('a', 1);
insert into store.ckey (name, id) VALUES ('b', 2);

CREATE TABLE IF NOT EXISTS store.target (
    id int primary key,
    uuid uuid,
    "Primary" varchar,
    boolean boolean,
    smallint smallint,
    int8 int,
    bigint bigint,
    numeric decimal,
    float float,
    date date,
    timestamp timestamp,
    rem text,
    image blob,
    gender varchar);

truncate table store.target;

CREATE TABLE IF NOT EXISTS store.ttoken (id int primary key, tr_begin bigint, tr_end bigint, ttoken bigint);