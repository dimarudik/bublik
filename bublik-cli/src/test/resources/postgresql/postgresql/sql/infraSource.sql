create table public.s (
    id1 int,
    id2 int,
    name varchar(256)
);

insert into public.s (id1, id2, name)
select n as id1, n * -1 as id2, 'PostgreSQL ' || n as name
from generate_series(1, 4210023) as n;

analyze public.s;

-- create table public.big (image bytea);
-- insert into public.big (image) values (repeat('X', 256000000)::bytea);
-- analyze public.big;
