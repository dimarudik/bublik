create table public.t1 (
    uuid uuid,
    id char(32),
    name varchar(256));
create table public.t2 (
   uuid uuid,
   id char(32),
   name varchar(256));

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
