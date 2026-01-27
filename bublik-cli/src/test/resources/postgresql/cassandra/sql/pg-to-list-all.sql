create table public.to_list (
    id int,
    parent_id int,
    user_name varchar,
    email varchar,
    last_update timestamp,
    v1 varchar,
    v2 varchar);

insert into public.to_list (id, parent_id, user_name, email, last_update, v1, v2)
    select id, parent_id, user_name,
        user_name || '@' ||
               (case (random() * 3)::integer
                   when 0 then 'gmail'
                   when 1 then 'hotmail'
                   when 2 then 'yahoo'
                   when 3 then 'yandex'
               end) || '.com' as email,
       now() - random() * (timestamp '2025-01-31 00:00:00' - timestamp '2025-01-01 23:59:59') as last_update,
       v1,
       v2
    from (
        select
            num as id,
            floor(random() * 100000 + 1)::int as parent_id,
            substr(md5(random()::text), 1, 10) as user_name,
            substr(md5(random()::text), 1, 10) as v1,
            substr(md5(random()::text), 1, 10) as v2
        from generate_series(1, 100000) as num
        );

analyze public.to_list;
