create table if not exists public.p_src (
    id int,
    created timestamp not null,
    name text,
    amount bigint,
    shard_key int
) partition by range (created);
alter table public.p_src add primary key (id, created);
create table if not exists public.p_src_def partition of public.p_src default;
create table if not exists public.p_src_202509 partition of public.p_src for values from ('2025-09-01 00:00:00') to ('2025-09-30 23:59:59');
create table if not exists public.p_src_202510 partition of public.p_src for values from ('2025-10-01 00:00:00') to ('2025-10-31 23:59:59');
create table if not exists public.p_src_202511 partition of public.p_src for values from ('2025-11-01 00:00:00') to ('2025-11-30 23:59:59');

insert into public.p_src (id, created, name, amount, shard_key)
    select num as id,
            timestamp '2025-08-15 00:00:00' + random() * (timestamp '2025-11-30 23:59:59' - timestamp '2025-09-01 00:00:00') as created,
            'Name ' || substr(md5(random()::text), 1, 10) as name,
            floor(random() * 1000000)::bigint as amount,
            1 as shard_key
    from generate_series(1, 1900000) as num;

analyze public.p_src ;
