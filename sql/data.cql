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

create table store.user (
    user_id int,
    item_id int,
    user_name varchar static,
    email varchar static,
    item_name varchar,
    description text,
    primary key (user_id, item_id));

create table store.item (
    item_id int,
    user_id int,
    item_name varchar static,
    description text static,
    user_name varchar,
    email varchar,
    primary key (item_id, user_id));

-- expand on
-- select user_name, email, item_name, description from store.user where user_id = 800;
-- select item_name, description, user_name, email from store.item where item_id = 300;

-- create table store.p_ts (id timestamp, name varchar, phone varchar, description varchar, primary key (id));
-- insert into store.p_ts (id, name, phone, description) values ('2022-10-30T09:15:30', 'Max', '+79011000101', 'The best man');
-- select token(id), id, name, phone, description from store.p_ts;
-- select column_name, type, position from system_schema.columns where keyspace_name = 'store' and table_name = 'p_ts' and kind = 'partition_key' allow filtering;

-- create table store.p_uuid (id uuid, name varchar, phone varchar, description varchar, primary key (id));
-- insert into store.p_uuid (id, name, phone, description) values (550e8400-e29b-41d4-a716-446655440000, 'Max', '+79011000101', 'The best man');
-- select token(id), id, name, phone, description from store.p_uuid;
-- select column_name, type, position from system_schema.columns where keyspace_name = 'store' and table_name = 'p_uuid' and kind = 'partition_key' allow filtering;

-- create table store.p_smallint (id smallint, name varchar, phone varchar, description varchar, primary key (id));
-- insert into store.p_smallint (id, name, phone, description) values (1000, 'Max', '+79011000101', 'The best man');
-- select token(id), id, name, phone, description from store.p_smallint;
-- select column_name, type, position from system_schema.columns where keyspace_name = 'store' and table_name = 'p_smallint' and kind = 'partition_key' allow filtering;

-- create table store.p_int (id int, name varchar, phone varchar, description varchar, primary key (id));
-- insert into store.p_int (id, name, phone, description) values (1000, 'Max', '+79011000101', 'The best man');
-- select token(id), id, name, phone, description from store.p_int;
-- select column_name, type, position from system_schema.columns where keyspace_name = 'store' and table_name = 'p_int' and kind = 'partition_key' allow filtering;

-- create table store.p_bigint (id bigint, name varchar, phone varchar, description varchar, primary key (id));
-- insert into store.p_bigint (id, name, phone, description) values (1000, 'Max', '+79011000101', 'The best man');
-- select token(id), id, name, phone, description from store.p_bigint;
-- select column_name, type, position from system_schema.columns where keyspace_name = 'store' and table_name = 'p_bigint' and kind = 'partition_key' allow filtering;

-- create table store.p_string (name varchar, id bigint, phone varchar, description varchar, primary key (name));
-- insert into store.p_string (id, name, phone, description) values (1000, 'Max', '+79011000101', 'The best man');
-- select token(name), id, name, phone, description from store.p_string;
-- select column_name, type, position from system_schema.columns where keyspace_name = 'store' and table_name = 'p_string' and kind = 'partition_key' allow filtering;

-- create table store.p_int_string (id bigint, name varchar, phone varchar, description varchar, primary key ((id, name)));
-- insert into store.p_int_string (id, name, phone, description) values (1000, 'Max', '+79011000101', 'The best man');
-- select token(id, name), id, name, phone, description from store.p_int_string;
-- select column_name, type, position from system_schema.columns where keyspace_name = 'store' and table_name = 'p_int_string' and kind = 'partition_key' allow filtering;

-- create table store.p_string_int (name varchar, id bigint, phone varchar, description varchar, primary key ((name, id)));
-- insert into store.p_string_int (id, name, phone, description) values (1000, 'Max', '+79011000101', 'The best man');
-- select token(name, id), id, name, phone, description from store.p_string_int;
-- select column_name, type, position from system_schema.columns where keyspace_name = 'store' and table_name = 'p_string_int' and kind = 'partition_key' allow filtering;

-- create table store.p_int_bigint_string (id int, bid bigint, name varchar, phone varchar, description varchar, primary key ((id, bid, name)));
-- insert into store.p_int_bigint_string (id, bid, name, phone, description) values (1000, 1000, 'Max', '+79011000101', 'The best man');
-- select token(id, bid, name), id, bid, name, phone, description from store.p_int_bigint_string;
-- select column_name, type, position from system_schema.columns where keyspace_name = 'store' and table_name = 'p_int_bigint_string' and kind = 'partition_key' allow filtering;

-- create table store.p_int_uuid (id bigint, uuid uuid, phone varchar, description varchar, primary key ((id, uuid)));
-- insert into store.p_int_uuid (id, uuid, phone, description) values (1000, 550e8400-e29b-41d4-a716-446655440000, '+79011000101', 'The best man');
-- select column_name, type, position from system_schema.columns where keyspace_name = 'store' and table_name = 'p_int_uuid' and kind = 'partition_key' allow filtering;

-- select token(id), id, name, phone, description from store.p_uuid;
-- select token(id, name), id, name, phone, description from store.p_int_string;