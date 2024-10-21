create role test with login superuser password 'test';
create type mood AS ENUM ('sad', 'ok', 'happy');
create type gender AS ENUM ('male', 'female', 'NA');

create table table1 (
    id bigint,
    create_at timestamp(6) with time zone,
    level character varying(255),
    update_at timestamp(6) with time zone,
    gender boolean,
    byteablob bytea,
    textclob text,
    "CaseSensitive" varchar(20),
    country_name varchar(256),
    rawbytea bytea,
    doc jsonb,
    uuid uuid,
    clobjsonb jsonb,
    current_mood mood,
    currency_name varchar(256)
);
create table "TABLE2" (
    id bigint,
    create_at timestamp(6) with time zone,
    level character varying(255),
    update_at timestamp(6) with time zone,
    gender boolean,
    byteablob bytea,
    textclob text,
    "CaseSensitive" varchar(20),
    tstzrange tstzrange
);
create table "Source" (
    id int primary key generated always as identity,
    uuid uuid,
    "Primary" varchar(256),
    boolean boolean,
    int2 int2,
    int4 int4,
    int8 int8,
    smallint smallint,
    bigint bigint,
    numeric numeric,
    float8 float8,
    date date,
    timestamp timestamp,
    timestamptz timestamptz,
    description text,
    image bytea,
    current_mood mood,
    time time
);
create table token (
    id int,
    tr_begin bigint,
    tr_end bigint,
    token bigint
);
create table target as
select
    id,
    uuid,
    "Primary",
    boolean,
    int2,
    int4,
    int8,
    smallint,
    bigint,
    numeric num,
    float8,
    date,
    timestamp,
    timestamptz,
    description as rem,
    image,
    current_mood,
    time time
 from "Source" where 0 = 1;
alter table target add column gender gender;
create table parted (
    id bigint,
    create_at timestamp(6) not null,
    name varchar(1000));
create table noc2c1 (
    id bigint primary key generated always as identity,
    name varchar);
create table noc2c2 (
    id bigint,
    name varchar);
create table intervals (
  id             int,
  time_period_1  INTERVAL,
  time_period_2  INTERVAL,
  time_period_3  INTERVAL,
  time_period_4  INTERVAL DAY TO SECOND(6)
);
create table sec_plain_text (
    id bigint primary key generated always as identity,
    pan varchar(25),
    first_name varchar(256),
    last_name varchar(256),
    expired date,
    pan_decrypted varchar(1024),
    all_decrypted varchar(1024)
);
create table sec_encrypted (
    id bigint,
    first_name varchar(256),
    last_name varchar(256),
    expired date,
    pan_encrypted_data varchar(512),
    pan_encryption_metadata jsonb,
    all_encrypted_data varchar(512),
    all_encryption_metadata jsonb,
    last_name_encrypted_data jsonb
);

insert into "Source" (uuid, "Primary", boolean,
        int2, int4, int8, smallint, bigint, numeric, float8,
        date, timestamp, timestamptz, description, image, current_mood, time)
    select gen_random_uuid() uuid, 'PostgreSQL ' || n name, case when mod(n, 2) = 0 then false else true end boolean,
        0 as int2, n as int4, n as int8, 10 as smallint, n as bigint, n / pi() as numeric, n / pi() as float8,
        current_date, current_timestamp, current_timestamp,
        rpad('PostgreSQL', 1000, '*') description,
        case when mod(n, 1000) = 0 then pg_read_binary_file('/var/lib/postgresql/bublik.png')::bytea end image,
        case
            when floor(random() * (3 + 1) + 0)::int = 1 then 'sad'::mood
            when floor(random() * (3 + 1) + 0)::int = 2 then 'ok'::mood
            when floor(random() * (3 + 1) + 0)::int = 2 then 'happy'::mood
            else null end as current_mood,
        now() time
    from generate_series(1, 100000) as n;
insert into "Source" (uuid, "Primary", boolean,
        int2, int4, int8, smallint, bigint, numeric, float8,
        date, timestamp, timestamptz, description, current_mood, time)
    select gen_random_uuid() uuid, 'PostgreSQL ' || n name, case when mod(n, 2) = 0 then false else true end boolean,
        0 as int2, n as int4, n as int8, 10 as smallint, n as bigint, n / pi() as numeric, n / pi() as float8,
        current_date, current_timestamp, current_timestamp,
        rpad('PostgreSQL', 1000, '*') description,
        case
            when floor(random() * (3 + 1) + 0)::int = 1 then 'sad'::mood
            when floor(random() * (3 + 1) + 0)::int = 2 then 'ok'::mood
            when floor(random() * (3 + 1) + 0)::int = 2 then 'happy'::mood
            else null end as current_mood,
        now() time
    from generate_series(1,900000) as n;
insert into sec_plain_text (pan, first_name, last_name, expired) values ('52132400010107395', 'NELSON', 'MANDELA', current_date);
insert into sec_plain_text (pan, first_name, last_name, expired) values ('52132400021325252', 'CHE', 'GUEVARA', current_date);
--vacuum "Source";
insert into noc2c1 (name)
    select rpad('PostgreSQL' || n, 100, '*') name from generate_series(1,100000) as n;

create table vacuum_me (
    id int primary key generated always as identity,
    uuid uuid,
    boolean boolean,
    int2 int2,
    int4 int4,
    int8 int8,
    smallint smallint,
    bigint bigint,
    float8 float8,
    date date,
    timestamp timestamp,
    timestamptz timestamptz
);

create table users (
    user_id int,
    user_name varchar,
    email varchar,
    primary key (user_id));
create unique index on users (user_name);

create table items (
    item_id int,
    item_name varchar,
    description text,
    primary key (item_id));
create unique index on items (item_name);

create table likes (
    like_id int generated by default as identity,
    user_id int references users,
    item_id int references items,
    primary key (like_id));
create unique index on likes (user_id, item_id);

-- select * from users where user_id = 800 \gx
-- select i.item_id, i.item_name, i.description from items i, likes l where i.item_id = l.item_id and l.user_id = 800 \gx
-- select * from items where item_id = 300 \gx
-- select u.user_id, u.user_name, u.email from users u, likes l where u.user_id = l.user_id and  l.item_id = 300 \gx

--insert into vacuum_me (uuid, boolean,
--        int2, int4, int8, smallint, bigint, float8, date, timestamp, timestamptz)
--    select gen_random_uuid() uuid, case when mod(n, 2) = 0 then false else true end boolean,
--        0 as int2, n as int4, n as int8, 10 as smallint, n as bigint, n / pi() as float8,
--        current_date, current_timestamp, current_timestamp
--    from generate_series(1,9000000) as n;

--vacuum verbose vacuum_me;
--alter table vacuum_me set (autovacuum_enabled = false);
--update vacuum_me set int8 = int8 where ctid >= '(0,1)' and ctid < '(43171,1)';
--vacuum verbose vacuum_me;
--update vacuum_me set int8 = int8 where ctid >= '(128610,1)';
--update ctid_chunks c set rows = (select count(1) from vacuum_me where ctid >= concat('(',c.start_page,',1)')::tid and ctid < concat('(',c.end_page,',1)')::tid );
