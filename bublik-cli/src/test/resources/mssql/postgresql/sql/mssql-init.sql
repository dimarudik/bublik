create database test;
use test;
create schema test;

create table test.t1
(
    uuid UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
    id int,
--     id char(32),
    name nvarchar(256),
    constraint pk_t1 primary key (uuid, id)
);
;WITH NumberSequence AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM NumberSequence WHERE n < 1000101
)
 INSERT INTO test.t1 (id, name)
SELECT n,
--        CAST(n AS VARCHAR(32)),
       'Name_' + CAST(n AS VARCHAR(10))
FROM NumberSequence OPTION (MAXRECURSION 0);

create table test.t2
(
    uuid UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
--     id int,
    id char(32) not null ,
    name nvarchar(256),
    constraint pk_t2 primary key nonclustered (uuid),
    index t2_uuid_id_idx clustered (id, uuid desc)
);
;WITH NumberSequence AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM NumberSequence WHERE n < 1000226
)
 INSERT INTO test.t2 (id, name)
SELECT n,
--     CAST(n AS VARCHAR(32)),
    'Name_' + CAST(n AS VARCHAR(10))
FROM NumberSequence OPTION (MAXRECURSION 0);

create table test.test
(
    id int primary key,
    a  tinyint,
    b  smallint,
    c  bigint,
    d  bit,
    e  decimal(10, 2),
    f  numeric(20, 3),
    g  money,
    h  smallmoney,
    i  float,
    j  real,
    k  date,
    l  time,
    m  datetime,
    n  datetime2,
    o  datetimeoffset,
    p  smalldatetime,
    q  char,
    r  varchar(256),
    s  text,
    t  nchar(256),
    u  nvarchar(256),
    v  ntext,
    w  binary(256),
    x  varbinary(256),
    y  image,
    z  json
);
insert into test.test (id,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,z)
values (1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, '2023-01-01', '00:00:00', '2023-01-01 00:00:00',
        '2023-01-01 00:00:00', '2023-01-01 00:00:00',
        '2023-01-01 00:00:00', 'q',
        'varchar', 'text', 'nchar', 'nvarchar', 'ntext', CONVERT(VARBINARY(MAX), 'Hello World'), 0x0123456789ABCDEF,
        '{"key": "value"}');
update test.test set y = (SELECT * FROM OPENROWSET(BULK '/bublik.png', SINGLE_BLOB) AS x) where id = 1;