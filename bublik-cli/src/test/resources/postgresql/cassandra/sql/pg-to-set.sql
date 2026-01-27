create table public.to_list (
    id int,
    parent_id int,
    user_name varchar,
    email varchar,
    last_update timestamp,
    v1 varchar,
    v2 varchar);

insert into public.to_list (id, parent_id, user_name, email, last_update, v1, v2)
values
(1, 1, 'user1', 'user1@gmail.com', '2025-01-01 00:00:00', 'v1', 'v2');

analyze public.to_list;
