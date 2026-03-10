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
   a varchar(256),
   b bigint,
   c bytea,
   d boolean,
   e date,
   f decimal,
   g double precision,
--    h duration,
   i float,
   j inet,
   k int,
   l smallint,
   m text,
   n time,
   o timestamp,
   p uuid,
   q int2,
   r uuid,
   s varchar(256),
   t bigint,
   u jsonb);
