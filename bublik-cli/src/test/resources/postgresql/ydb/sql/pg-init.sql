create table public.to_ydb (
    id int,
    name varchar(256));
insert into public.to_ydb (id, name)
   select num as id,
      'Item ' || substr(md5(random()::text), 1, 10) as name
      from generate_series(1, 500000) as num;
analyze public.to_ydb;

