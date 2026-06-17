create table public.a (
   "Id" int primary key,
   "Name" varchar(256));
create table public.b (
   uuid uuid primary key ,
   name varchar(256));
create table public.t1 (
    uuid uuid,
    id char(32),
    name varchar(256),
    constraint pk_t1 primary key (uuid, id));
create table public.t2 (
   uuid uuid,
   id char(32),
   name varchar(256));
create table public.t3 (
   id1 int,
   id2 int,
   name varchar(256));
create table public.t4 (
   id1 int,
   id2 int,
   name varchar(256),
   constraint pk_t4 primary key (id1, id2));

create table public.test (
     id int,
     a  smallint,
     b  int2,
     c  bigint,
     d  boolean,
     e  numeric(10, 2),
     f  decimal(20, 3),
     g  money,
--      h  smallmoney,
     i  double precision,
     j  real,
     k  date,
     l  time,
     m  timestamp,
     n  timestamp(3),
     o  timestamp with time zone,
--      p  smalldatetime,
     q  char,
     r  varchar(256),
     s  text,
     t  char(256),
     u  varchar(256),
     v  text,
     w  bytea,
     x  bytea,
     y  bytea,
     z  jsonb
);
