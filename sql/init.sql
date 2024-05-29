create role test with login superuser password 'test';
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
    uuid uuid
);
create table "TABLE2" (
    id bigint,
    create_at timestamp(6) with time zone,
    level character varying(255),
    update_at timestamp(6) with time zone,
    gender boolean,
    byteablob bytea,
    textclob text,
    "CaseSensitive" varchar(20)
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
    image bytea
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
    numeric,
    float8,
    date,
    timestamp,
    timestamptz,
    description as rem,
    image
 from "Source" where 0 = 1;
insert into "Source" (uuid, "Primary", boolean,
        int2, int4, int8, smallint, bigint, numeric, float8,
        date, timestamp, timestamptz, description, image)
    select gen_random_uuid() uuid, 'PostgreSQL ' || n name, case when mod(n, 2) = 0 then false else true end boolean,
        0 as int2, n as int4, n as int8, 10 as smallint, n as bigint, n / pi() as numeric, n / pi() as float8,
        current_date, current_timestamp, current_timestamp,
        rpad('PostgreSQL', 1000, '*') description,
        case when mod(n, 1000) = 0 then pg_read_binary_file('/var/lib/postgresql/bublik.png')::bytea
        end image
    from generate_series(1,100000) as n;
vacuum "Source";
create table parted (
    id bigint,
    create_at timestamp(6) not null,
    name varchar(1000));

