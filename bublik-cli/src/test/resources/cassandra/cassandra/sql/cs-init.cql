CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

CREATE OR REPLACE FUNCTION test.newTtlByTimestamp(last_update TIMESTAMP, ttl INT)
    RETURNS NULL ON NULL INPUT RETURNS INT
    LANGUAGE java AS $$ return (int)Math.max(0,ttl - ((System.currentTimeMillis() - last_update.getTime())/1000));$$;

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

create table test.t4 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));

create type test.encrypted_data (
    secret_data text,
    vector text
);

create table test.t5 (
    id int,
    uid int,
    v1 int,
    v2 tinyint,
    v3 smallint,
    v4 bigint,
    v5 text,
    v6 date,
    v7 timestamp,
    v8 boolean,
    v9 blob,
    v10 float,
    v11 decimal,
    v12 uuid,
    v13 set<text>,
    v14 frozen<set<int>>,
    v15 list<text>,
    v16 frozen<list<int>>,
    v17 map<text, text>,
    v18 frozen<map<int, int>>,
    v19 frozen<encrypted_data>,
    v20 frozen<map<text, text>>,
    primary key (id, uid));

insert into test.t5 (id, uid, v1) values (1, 0, 1) using ttl 1000 and timestamp 10;
insert into test.t5 (id, uid, v2) values (1, 0, 2) using ttl 2000 and timestamp 20;
insert into test.t5 (id, uid, v3) values (1, 0, 3) using ttl 3000 and timestamp 30;
insert into test.t5 (id, uid, v4) values (1, 0, 4) using ttl 4000 and timestamp 40;
insert into test.t5 (id, uid, v5) values (1, 0, 'a') using ttl 5000 and timestamp 50;
insert into test.t5 (id, uid, v6) values (1, 0, toDate(now())) using ttl 6000 and timestamp 60;
insert into test.t5 (id, uid, v7) values (1, 0, toTimestamp(now())) using ttl 7000 and timestamp 70;
insert into test.t5 (id, uid, v8) values (1, 0, true) using ttl 8000 and timestamp 80;
insert into test.t5 (id, uid, v10) values (1, 0, 3.14) using ttl 10000 and timestamp 100;
insert into test.t5 (id, uid, v11) values (1, 0, 5.45) using ttl 11000 and timestamp 110;
insert into test.t5 (id, uid, v12) values (1, 0, ed584e99-80f7-4b13-9a90-9dc5571e6821) using ttl 12000 and timestamp 120;
insert into test.t5 (id, uid, v13) values (1, 0, {'k1', 'k2', 'k3'}) using ttl 13000 and timestamp 130;
insert into test.t5 (id, uid, v14) values (1, 0, {1, 2, 3}) using ttl 14000 and timestamp 140;
insert into test.t5 (id, uid, v15) values (1, 0, ['k1', 'k2', 'k3']) using ttl 15000 and timestamp 150;
insert into test.t5 (id, uid, v16) values (1, 0, [1, 2, 3]) using ttl 16000 and timestamp 160;
insert into test.t5 (id, uid, v17) values (1, 0, {'k1' : 'v1', 'k2' : 'v1', 'k3' : 'v2'}) using ttl 17000 and timestamp 170;
insert into test.t5 (id, uid, v18) values (1, 0, {1 : 1, 2 : 1, 3 : 2}) using ttl 18000 and timestamp 180;
insert into test.t5 (id, uid, v19) values (1, 0, {secret_data : 'secret_data', vector : 'vector'}) using ttl 19000 and timestamp 190;
insert into test.t5 (id, uid, v20) values (1, 0, {'k1' : 'v1', 'k2' : 'v1', 'k3' : 'v2'}) using ttl 20000 and timestamp 200;

create table test.t6 (id tinyint,    v1 int, primary key (id));
insert into test.t6  (id, v1) values (1, 1) using ttl 1000 and timestamp 10;
create table test.t7 (id smallint,   v1 int, primary key (id));
insert into test.t7  (id, v1) values (1, 1) using ttl 1000 and timestamp 10;
create table test.t8 (id int,        v1 int, primary key (id));
insert into test.t8  (id, v1) values (1, 1) using ttl 1000 and timestamp 10;
create table test.t9 (id bigint,     v1 int, primary key (id));
insert into test.t9  (id, v1) values (1, 1) using ttl 1000 and timestamp 10;
create table test.t10 (id text,      v1 int, primary key (id));
insert into test.t10  (id, v1) values ('a', 1) using ttl 1000 and timestamp 10;
create table test.t11 (id timestamp, v1 int, primary key (id));
insert into test.t11  (id, v1) values (toTimestamp(now()), 1) using ttl 1000 and timestamp 10;
create table test.t12 (id uuid,      v1 int, primary key (id));
insert into test.t12  (id, v1) values (ed584e99-80f7-4b13-9a90-9dc5571e6821, 1) using ttl 1000 and timestamp 10;

create table test.log_time (
    id int,
    uid int,
    log_time timestamp,
    log_date date,
    primary key (id, uid));

insert into test.log_time (id, uid, log_time, log_date) values (1, 1, '2025-12-02T00:00:00.001+0000', toDate(now()));
insert into test.log_time (id, uid, log_time, log_date) values (2, 1, '2024-12-02T00:00:00.001+0000', toDate(now()));
insert into test.log_time (id, uid, log_time, log_date) values (3, 1, '2024-10-02T00:00:00.001+0000', toDate(now()));

create table test.no_columns (
    id int,
    uid int,
    v1 int,
    primary key (id, uid));
insert into test.no_columns (id, uid, v1) values (1, 1, 1);
insert into test.no_columns (id, uid, v1) values (2, 2, 2);

create table test.filter1 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));
insert into test.filter1 (id, uid, v1, v2, v3, v4) values (1, 0, 11, 21, 30, 'a');
insert into test.filter1 (id, uid, v1, v2, v3, v4) values (1, 2, 11, 21, 32, 'a');
insert into test.filter1 (id, uid, v1, v2, v3, v4) values (2, 1, 11, 21, 36, 'a');
insert into test.filter1 (id, uid, v1, v2, v3, v4) values (2, 2, 11, 21, 37, 'a');

create table test.t_blob (
    key blob,
    value blob,
    v blob,
    primary key (key, value));

insert into test.t_blob (key, value, v)
values
    (0x0102030405060708090a0b0c0d0e0f101112131415161718 , 0x0102030405060708090a0b0c0d0e0f101112131415161718, 0x0102030405060708090a0b0c0d0e0f101112131415161718);
