create type mood AS ENUM ('sad', 'ok', 'happy');
create type gender AS ENUM ('male', 'female', 'NA');
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
    j json
);
insert into public."Source" (uuid, "Primary", boolean,
        int2, int4, int8, smallint, bigint, numeric, float8,
        date, timestamp, timestamptz, description
        , image
        , current_mood, time, j)
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
        '{"key": "value"}' j
    from generate_series(1, 50000) as n;
insert into public."Source" (uuid, "Primary", boolean,
        int2, int4, int8, smallint, bigint, numeric, float8,
        date, timestamp, timestamptz, description, current_mood, time, j)
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
        '{"key": "value"}' j
    from generate_series(1,500000) as n;
analyze public."Source" ;
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
    j
 from public."Source" where 0 = 1;
