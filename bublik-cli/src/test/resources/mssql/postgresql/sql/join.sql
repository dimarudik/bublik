create database test;
use test;
create schema test;

-- A
create table test.a
(
    id int primary key,
    name nvarchar(256)
);
;WITH NumberSequence AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM NumberSequence WHERE n < 10
)
 INSERT INTO test.a (id, name)
SELECT n, 'Name_' + CAST(n AS VARCHAR(10))
FROM NumberSequence OPTION (MAXRECURSION 0);

-- A
create table test.b
(
    id int primary key,
    a_id int
);
;WITH NumberSequence AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM NumberSequence WHERE n < 310000
)
 INSERT INTO test.b (id, a_id)
SELECT n as id, n % 10 as a_id
FROM NumberSequence OPTION (MAXRECURSION 0);
