create table public.s (
    id1 int,
    id2 int,
    name varchar(256)
);

insert into public.s (id1, id2, name)
select n as id1, n * -1 as id2, 'PostgreSQL ' || n as name
from generate_series(1, 4210023) as n;

update public.s set name= null where id1 = 100000 and id2 = -100000;
update public.s set name= null where id1 = 200000 and id2 = -200000;
update public.s set name= null where id1 = 300000 and id2 = -300000;
update public.s set name= null where id1 = 400000 and id2 = -400000;

analyze public.s;

-- create table public.big (image bytea);
-- insert into public.big (image) values (repeat('X', 256000000)::bytea);
-- analyze public.big;
