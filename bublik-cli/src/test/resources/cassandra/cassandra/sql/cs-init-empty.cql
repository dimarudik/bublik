--CREATE ROLE test WITH PASSWORD = 'test' AND SUPERUSER = true AND LOGIN = true;
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
CREATE KEYSPACE IF NOT EXISTS test2 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create table test.t1 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));

create table test.t2 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));

create table test.t3 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));

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

create table test.t6 (id tinyint,    v1 int, primary key (id));
create table test.t7 (id smallint,   v1 int, primary key (id));
create table test.t8 (id int,        v1 int, primary key (id));
create table test.t9 (id bigint,     v1 int, primary key (id));
create table test.t10 (id text,      v1 int, primary key (id));
create table test.t11 (id timestamp, v1 int, primary key (id));
create table test.t12 (id uuid,      v1 int, primary key (id));

create type test2.encrypted_data (
    secret_data text,
    vector text
);

create table test2.t5 (
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
    v20 frozen<map<text, text>>,
    primary key (id, uid));

create table test2.log_time (
    id int,
    uid int,
    log_time timestamp,
    log_date date,
    primary key (id, uid));

create table test.no_columns (
    id int,
    uid int,
    v1 int,
    primary key (id, uid));

create table test.filter1 (
    id int,
    uid int,
    v1 int,
    v2 int,
    v3 int static,
    v4 varchar,
    primary key (id, uid));

create table test.t_blob (
    key blob,
    value blob,
    v blob,
    primary key (key, value));
