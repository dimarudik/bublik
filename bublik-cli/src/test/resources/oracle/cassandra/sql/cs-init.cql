--CREATE ROLE test WITH PASSWORD = 'test' AND SUPERUSER = true AND LOGIN = true;
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create table test.user (
    user_id int,
    item_id int,
    user_name varchar static,
    email varchar static,
    item_name varchar,
    description text,
    last_update timestamp,
    primary key (user_id, item_id));
create table test.item (
    item_id int,
    user_id int,
    item_name varchar static,
    description text static,
    user_name varchar,
    email varchar,
    last_update timestamp,
    primary key (item_id, user_id));
