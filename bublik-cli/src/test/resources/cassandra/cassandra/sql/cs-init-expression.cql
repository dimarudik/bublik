--CREATE ROLE test WITH PASSWORD = 'test' AND SUPERUSER = true AND LOGIN = true;
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create table test.expression (
    acc int,
    year text,
    op_id int,
    log_date timestamp,
    v1 int,
    primary key ((acc, year), op_id));
insert into test.expression (acc, year, op_id, log_date, v1) values (1, '2025', 1, '2025-02-02T00:00:00.001+0000', 1);

CREATE OR REPLACE FUNCTION test.extract_month (input TIMESTAMP)
     RETURNS NULL ON NULL INPUT RETURNS TEXT
     LANGUAGE java AS 'Calendar calendar = Calendar.getInstance(); calendar.setTime(input); int month = calendar.get(Calendar.MONTH) + 1; return month < 10 ? "0" + month : String.valueOf(month);';
CREATE OR REPLACE FUNCTION test.extract_year (input TIMESTAMP)
     RETURNS NULL ON NULL INPUT RETURNS TEXT
     LANGUAGE java AS 'Calendar calendar = Calendar.getInstance(); calendar.setTime(input); int year = calendar.get(Calendar.YEAR); return year + "";';
CREATE OR REPLACE FUNCTION test.concat (s1 TEXT, s2 TEXT)
     RETURNS NULL ON NULL INPUT RETURNS TEXT
     LANGUAGE java AS 'return s1 + s2;';
