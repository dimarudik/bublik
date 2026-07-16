--create role test with login superuser password 'test';
create extension hstore;
create schema if not exists test;
create type mood AS ENUM ('sad', 'ok', 'happy');
create type gender AS ENUM ('male', 'female', 'NA');

create table public.empty_table (id int primary key,name varchar(256));
insert into public.empty_table (id, name)
    select n as id, 'PostgreSQL ' || n as name
        from generate_series(1, 350000) as n;
analyze public.empty_table;
delete from public.empty_table where id between 40000 and 310000;
create table test.empty_table (id int,name varchar(256));

create table test.a (
    a int,
    b numeric(10,0));
insert into test.a (a, b) values (1, 1);
create table test.b (
    b numeric(10,0),
    a int);

create table test.table1 (
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
create table public."TABLE2" (
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
create table public.table3 (
    id bigint,
    create_at timestamp(6) with time zone,
    level character varying(255),
    update_at timestamp(6) with time zone,
    gender boolean,
    byteablob bytea,
    textclob text,
    "CaseSensitive" varchar(20),
    country_name varchar(256) not null,
    rawbytea bytea,
    doc jsonb,
    uuid uuid,
    clobjsonb jsonb,
    current_mood mood,
    currency_name varchar(256)
);
create table public."Source" (
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
    time time,
    j json,
    ip inet,
    h hstore,
    ints _int8
);
create table public.token (
    id int,
    tr_begin bigint,
    tr_end bigint,
    token bigint
);
create table public.target as
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
    time as time,
    j,
    ip,
    h,
    ints
 from public."Source" where 0 = 1;
alter table public.target add column gender gender;
-- alter table public.target add primary key (id);
create table public.parted (
    id bigint,
    create_at timestamp(6) not null,
    name varchar(1000));
create table public.noc2c1 (
    id bigint primary key generated always as identity,
    name varchar);
create table public.noc2c2 (
    id bigint,
    name varchar);
create table public.intervals (
  id             int,
  time_period_1  INTERVAL,
  time_period_2  INTERVAL,
  time_period_3  INTERVAL,
  time_period_4  INTERVAL DAY TO SECOND(6)
);

insert into public."Source" (uuid, "Primary", boolean,
        int2, int4, int8, smallint, bigint, numeric, float8,
        date, timestamp, timestamptz, description
        , image, current_mood, time, j, ip, h, ints)
    select gen_random_uuid() as uuid, 'PostgreSQL ' || n as name,
        case when mod(n, 2) = 0 then false else true end as boolean,
        0 as int2, n as int4, n as int8, 10 as smallint, n as bigint, n / pi() as numeric, n / pi() as float8,
        current_date, current_timestamp, current_timestamp,
        rpad('PostgreSQL', 1000, '*') as description
        ,case when mod(n, 1000) = 0 then pg_read_binary_file('/var/lib/postgresql/bublik.png')::bytea end image
        ,case
            when floor(random() * (3 + 1) + 0)::int = 1 then 'sad'::mood
            when floor(random() * (3 + 1) + 0)::int = 2 then 'ok'::mood
            when floor(random() * (3 + 1) + 0)::int = 2 then 'happy'::mood
            else null end as current_mood,
        now() as time,
        '{"key": "value"}' j,
        case when mod(n, 2) = 0 then '192.168.2.1'::inet else '2001:0db8:85a3:0000:0000:8a2e:0370:7334'::inet end as ip,
        '"a"=>"1","b"=>"2"'::hstore h,
        case when mod(n, 5) <> 0 then '{ 14, 2, 3, 100, 10963 }'::_int8 else null end as ints
    from generate_series(1, 10000) as n;
insert into public."Source" (uuid, "Primary", boolean,
        int2, int4, int8, smallint, bigint, numeric, float8,
        date, timestamp, timestamptz, description, current_mood, time, j, ip, h, ints)
    select gen_random_uuid() uuid, 'PostgreSQL ' || n name, case when mod(n, 2) = 0 then false else true end boolean,
        0 as int2, n as int4, n as int8, 10 as smallint, n as bigint, n / pi() as numeric, n / pi() as float8,
        current_date, current_timestamp, current_timestamp,
        rpad('PostgreSQL', 100, '*') description,
        case
            when floor(random() * (3 + 1) + 0)::int = 1 then 'sad'::mood
            when floor(random() * (3 + 1) + 0)::int = 2 then 'ok'::mood
            when floor(random() * (3 + 1) + 0)::int = 2 then 'happy'::mood
            else null end as current_mood,
        now() time,
        '{"key": "value"}' j,
        case when mod(n, 2) = 0 then '192.168.2.1'::inet else '2001:0db8:85a3:0000:0000:8a2e:0370:7334'::inet end as ip,
        'c=>3,d=>3'::hstore h,
        case when mod(n, 5) <> 0 then '{ 14, 2, 3, 100, 10963 }'::_int8 else null end as ints
    from generate_series(1,300000) as n;

analyze public."Source" ;

create table public.not_null_failure (
    id int,
    name varchar(256),
    "table" bigint,
    "Table" bigint,
    "c" int);

insert into public.not_null_failure (id, name, "table", "Table", "c")
   select num as id,
      'Item ' || substr(md5(random()::text), 1, 10) as name,
      num "table",
      num as "Table",
      num as "c"
      from generate_series(1, 100000) as num;
update public.not_null_failure set name = null where id = 1000;
analyze public.not_null_failure;

create table test.not_null_failure (
    id int,
    name varchar(256) not null,
    "table" bigint,
    "Table" bigint,
    "c" int);

create table public.users (
    id int,
    user_name varchar,
    email varchar,
    touch_count int default 0,
    last_update timestamp,
    primary key (id));
create unique index on public.users (user_name);
create unique index on public.users (user_name, email);

create table public.items (
    id int,
    item_name varchar,
    description text,
    touch_count int default 0,
    last_update timestamp,
    primary key (id));
--create unique index on public.items (item_name);

create table public.likes (
    id int generated by default as identity,
    user_id int references users,
    item_id int references items,
    touch_count int default 0,
    r char(200),
    last_update timestamp,
    primary key (id));
create unique index on public.likes (user_id, item_id);

insert into public.users (id, user_name, email)
    select id, user_name,
        user_name || '@' ||
               (case (random() * 3)::integer
                   when 0 then 'gmail'
                   when 1 then 'hotmail'
                   when 2 then 'yahoo'
                   when 3 then 'yandex'
               end) || '.com' as email
    from (
        select num as id, substr(md5(random()::text), 1, 10) as user_name
        from generate_series(1, 100000) as num
        );

insert into public.items (id, item_name, description)
    select num as id,
           'Item ' || substr(md5(random()::text), 1, 10) as item_name,
           'Description ' || substr(md5(random()::text), 1, 30) as description
    from generate_series(1, 100000) as num;

insert into public.likes (id, user_id, item_id, r)
    select num as id,
       floor(random() * 100000 + 1)::int as user_id,
       floor(random() * 100000 + 1)::int as item_id,
       rpad('Bublik is the best tool for migration ',50,'*') as r
    from generate_series(1, 500000) as num
on conflict (user_id, item_id) do nothing;

analyze public.users;
analyze public.items;
analyze public.likes;
--select setval('public.likes_id_seq', 1000001, false);

create table public.p_src (
    id int,
    created timestamp not null,
    name text,
    amount bigint,
    shard_key int,
    names varchar(30)[],
    texts text[]
) partition by range (created);
alter table public.p_src add primary key (id, created);
create table public.p_src_def partition of public.p_src default;
create table public.p_src_202501 partition of public.p_src for values from ('2025-01-01 00:00:00') to ('2025-01-31 23:59:59');
create table public.p_src_202509 partition of public.p_src for values from ('2025-09-01 00:00:00') to ('2025-09-30 23:59:59');
create table public.p_src_202510 partition of public.p_src for values from ('2025-10-01 00:00:00') to ('2025-10-31 23:59:59');
create table public.p_src_202511 partition of public.p_src for values from ('2025-11-01 00:00:00') to ('2025-11-30 23:59:59');
create table public.p_src_202512 partition of public.p_src for values from ('2025-12-01 00:00:00') to ('2025-12-31 23:59:59');

insert into public.p_src (id, created, name, amount, shard_key, names, texts)
    select num as id,
            timestamp '2025-01-01 00:00:00' + random() * (timestamp '2025-01-31 00:00:00' - timestamp '2025-01-01 23:59:59') as created,
            'Name ' || substr(md5(random()::text), 1, 10) as name,
            floor(random() * 1000000)::bigint as amount,
            random() * 1 as shard_key,
            case when num % 10 = 0 then array['A:' || substr(md5(random()::text), 1, 10),'B:' || substr(md5(random()::text), 1, 10),'C:' || substr(md5(random()::text), 1, 10)] else null end as names,
            case when num % 10 = 0 then array['D:' || substr(md5(random()::text), 1, 10),'E:' || substr(md5(random()::text), 1, 10),'F:' || substr(md5(random()::text), 1, 10)] else null end as texts
    from generate_series(1, 10) as num;

insert into public.p_src (id, created, name, amount, shard_key, names, texts)
    select num as id,
            timestamp '2025-08-15 00:00:00' + random() * (timestamp '2025-12-31 23:59:59' - timestamp '2025-09-01 00:00:00') as created,
            'Name ' || substr(md5(random()::text), 1, 10) as name,
            floor(random() * 1000000)::bigint as amount,
            random() * 1 as shard_key,
            case when num % 10 = 0 then array['A:' || substr(md5(random()::text), 1, 10),'B:' || substr(md5(random()::text), 1, 10),'C:' || substr(md5(random()::text), 1, 10)] else null end as names,
            case when num % 10 = 0 then array['D:' || substr(md5(random()::text), 1, 10),'E:' || substr(md5(random()::text), 1, 10),'F:' || substr(md5(random()::text), 1, 10)] else null end as texts
    from generate_series(1, 100000) as num;

analyze public.p_src ;

create table public.p_trg (
    id int,
    created timestamp not null,
    name text,
    amount bigint,
    shard_key int,
    names varchar(30)[],
    texts text[]
) partition by range (created);
alter table public.p_trg add primary key (id, created, shard_key);
create table public.p_trg_def partition of public.p_trg default partition by list (shard_key);
create table public.p_trg_202509 partition of public.p_trg for values from ('2025-09-01 00:00:00') to ('2025-09-30 23:59:59') partition by list (shard_key);
create table public.p_trg_202510 partition of public.p_trg for values from ('2025-10-01 00:00:00') to ('2025-10-31 23:59:59') partition by list (shard_key);
create table public.p_trg_202511 partition of public.p_trg for values from ('2025-11-01 00:00:00') to ('2025-11-30 23:59:59') partition by list (shard_key);
create table public.p_trg_202512 partition of public.p_trg for values from ('2025-12-01 00:00:00') to ('2025-12-31 23:59:59') partition by list (shard_key);

create table public.p_trg_202509_0 partition of public.p_trg_202509 for values in (0);
create table public.p_trg_202509_1 partition of public.p_trg_202509 for values in (1);
create table public.p_trg_202510_0 partition of public.p_trg_202510 for values in (0);
create table public.p_trg_202510_1 partition of public.p_trg_202510 for values in (1);
create table public.p_trg_202511_0 partition of public.p_trg_202511 for values in (0);
create table public.p_trg_202511_1 partition of public.p_trg_202511 for values in (1);
create table public.p_trg_202512_0 partition of public.p_trg_202512 for values in (0);
create table public.p_trg_202512_1 partition of public.p_trg_202512 for values in (1);

create sequence public.serialcolumn_seq;
create table public.serialcolumn (
    id bigint DEFAULT nextval('serialcolumn_seq') primary key,
    name varchar(256)
);

insert into public.serialcolumn (id, name)
    select num as id,
           'Description ' || substr(md5(random()::text), 1, 30) as name
    from generate_series(1, 100) as num;

create table test.serialcolumn (
    id bigint,
    name varchar(256)
);


create table public.s (
    id1 int,
    id2 int,
    name varchar(256),
    constraint pk_s primary key (id1, id2)
);

create table public.t (
    id1 int,
    id2 int,
    name varchar(256),
    constraint pk_t primary key (id1, id2)
);

insert into public.s (id1, id2, name)
select n as id1, n * -1 as id2, 'PostgreSQL ' || n as name
from generate_series(1, 510023) as n;

analyze public.s;

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

