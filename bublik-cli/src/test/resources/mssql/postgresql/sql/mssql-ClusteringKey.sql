create database test;
use test;
create schema test;

-- A
create table test.a
(
    "Id" int primary key,
    "Name" nvarchar(256)
);
;WITH NumberSequence AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM NumberSequence WHERE n < 200020
)
 INSERT INTO test.a ("Id", "Name")
SELECT n, 'Name_' + CAST(n AS VARCHAR(10))
FROM NumberSequence OPTION (MAXRECURSION 0);

-- B
create table test.b
(
    uuid UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID() primary key ,
    name nvarchar(256)
);
;WITH NumberSequence AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM NumberSequence WHERE n < 100999
)
 INSERT INTO test.b (name)
SELECT 'Name_' + CAST(n AS VARCHAR(10))
FROM NumberSequence OPTION (MAXRECURSION 0);

-- T1
create table test.t1
(
    uuid UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
    id char(32),
    name nvarchar(256),
    constraint pk_t1 primary key (uuid, id)
);
;WITH NumberSequence AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM NumberSequence WHERE n < 300101
)
 INSERT INTO test.t1 (id, name)
SELECT n,
--        CAST(n AS VARCHAR(32)),
       'Name_' + CAST(n AS VARCHAR(10))
FROM NumberSequence OPTION (MAXRECURSION 0);

-- T2
create table test.t2
(
    uuid UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
--     id int not null,
    id char(32) not null ,
    name nvarchar(256),
    constraint pk_t2 primary key nonclustered (uuid),
    index t2_uuid_id_idx clustered (id, uuid desc)
);
;WITH NumberSequence AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM NumberSequence WHERE n < 400092
)
 INSERT INTO test.t2 (id, name)
SELECT n,
       'Name_' + CAST(n AS VARCHAR(10))
FROM NumberSequence OPTION (MAXRECURSION 0);

-- T3
create table test.t3
(
    id1 int IDENTITY(1,1),
    id2 int,
    name nvarchar(256),
    constraint pk_t3 primary key (id1, id2 desc)
);
;WITH NumberSequence AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM NumberSequence WHERE n < 250001
)
 INSERT INTO test.t3 (id2, name)
SELECT n % 10,
       'Name_' + CAST(n AS VARCHAR(10))
FROM NumberSequence OPTION (MAXRECURSION 0);

-- T4
CREATE TABLE test.t4
(
    id1 int NOT NULL,
    id2 int NOT NULL,
    name nvarchar(256),
    CONSTRAINT pk_t4 PRIMARY KEY (id1, id2)
);
;WITH NumberSequence AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM NumberSequence WHERE n < 1060001
)
 INSERT INTO test.t4 (id1, id2, name)
SELECT
    (n - 1) / 500,
    (n - 1) % 500,
    'Name_' + CAST(n AS VARCHAR(10))
FROM NumberSequence OPTION (MAXRECURSION 0);
