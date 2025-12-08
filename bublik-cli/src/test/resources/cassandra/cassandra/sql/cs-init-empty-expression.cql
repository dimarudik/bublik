--CREATE ROLE test WITH PASSWORD = 'test' AND SUPERUSER = true AND LOGIN = true;
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create table test.expression (
    acc int,
    year_month text,
    op_id int,
    log_date timestamp,
    v1 int,
    primary key ((acc, year_month), op_id));
