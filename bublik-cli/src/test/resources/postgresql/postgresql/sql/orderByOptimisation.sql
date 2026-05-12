create table public.s (
    id1 int,
    id2 int,
    name varchar(256),
    constraint pk_s primary key (id1, id2)
);

create table public.t (
    id1 int,
    id2 int,
    name varchar(256),
    constraint pk_t primary key (id1, id2)
);

insert into public.s (id1, id2, name)
select n as id1, n * -1 as id2, 'PostgreSQL ' || n as name
    from generate_series(1, 510023) as n;

analyze public.s;