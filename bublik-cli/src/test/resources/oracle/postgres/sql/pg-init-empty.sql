--create role test with login superuser password 'test';
create schema if not exists test;
create type mood AS ENUM ('sad', 'ok', 'happy');
create type gender AS ENUM ('male', 'female', 'NA');

CREATE TABLE test.a (
    id int,
    a numeric(12,2),
    b numeric(10,0),
    c char(1),
    d char(10),
    "ALL" varchar(255),
    "LEVEL" varchar(255),
    e real,
    t timestamp,
    create_at timestamp(6) with time zone,
    gender smallint CHECK (gender IN (0,1)),
    byteablob bytea,
    textclob text,
    exclude_me int,
    "CaseSensitive" varchar(40),
    country_id int,
    rawbytea bytea,
    json_like varchar(4000),
    doc jsonb,
    "uuid" char(36),
    dd date
);
create table test.b (
    b numeric(10,0),
    a int primary key
);
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
    json_like jsonb,
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
create table public.parted1 (
    id bigint,
    create_at timestamp(6) not null,
    name varchar(1000));
create table public.parted2 (
    id bigint,
    create_at timestamp(6) not null,
    name varchar(1000));
create table public.parted3 (
    id bigint,
    create_at timestamp(6) not null,
    name varchar(1000) not null);
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

