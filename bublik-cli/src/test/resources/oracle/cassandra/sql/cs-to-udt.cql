CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create type test.complex_body (
    parent_id int,
    user_name text,
    email text,
    last_update timestamp
);

create table test.to_list (
    id int,
    b test.complex_body,
    body frozen<test.complex_body>,
    m map<int, text>,
    mf frozen<map<int, text>>,
    primary key (id));
