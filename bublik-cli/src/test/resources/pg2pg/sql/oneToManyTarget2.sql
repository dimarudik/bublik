create table public.p_trg (
    id int,
    created timestamp not null,
    name text,
    amount bigint,
    shard_key int
) partition by range (created);
alter table public.p_trg add primary key (id, created, shard_key);
create table public.p_trg_def partition of public.p_trg default partition by list (shard_key);
create table public.p_trg_202509 partition of public.p_trg for values from ('2025-09-01 00:00:00') to ('2025-09-30 23:59:59') partition by list (shard_key);
create table public.p_trg_202510 partition of public.p_trg for values from ('2025-10-01 00:00:00') to ('2025-10-31 23:59:59') partition by list (shard_key);
create table public.p_trg_202511 partition of public.p_trg for values from ('2025-11-01 00:00:00') to ('2025-11-30 23:59:59') partition by list (shard_key);

create table public.p_trg_202509_0 partition of public.p_trg_202509 for values in (0);
create table public.p_trg_202509_1 partition of public.p_trg_202509 for values in (1);
create table public.p_trg_202510_0 partition of public.p_trg_202510 for values in (0);
create table public.p_trg_202510_1 partition of public.p_trg_202510 for values in (1);
create table public.p_trg_202511_0 partition of public.p_trg_202511 for values in (0);
create table public.p_trg_202511_1 partition of public.p_trg_202511 for values in (1);
