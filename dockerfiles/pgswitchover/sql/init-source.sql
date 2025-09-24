create table public.switchover (
    id bigint,
    name varchar(512)
);

insert into public.switchover (id, name)
    select num as id,
            'Name ' || substr(md5(random()::text), 1, 512) as name
    from generate_series(1, 19000000) as num;

analyze public.switchover;
