create database test;
use test;
create schema test;

CREATE TABLE test.b (
    id            BIGINT PRIMARY KEY,
    a             DECIMAL(12,2),
    b             BIGINT,
    c             CHAR(1),
    d             NCHAR(10),
    [all]         NVARCHAR(255),
    level         VARCHAR(255),
    e             REAL,
    t             DATETIME,
    create_at     DATETIMEOFFSET(6),
    gender        TINYINT,
    byteablob     VARBINARY(MAX),
    textclob      NVARCHAR(MAX),
    exclude_me    BIGINT,
    casesensitive NVARCHAR(40),
    country_id    BIGINT,
    rawbytea      VARBINARY(MAX),
    json_like     NVARCHAR(MAX),
    doc           NVARCHAR(MAX),
    uuid          UNIQUEIDENTIFIER,
    int16_t       SMALLINT,
    int128_t      VARCHAR(40),
    int256_t      VARCHAR(80),
    bfloat16_t    REAL
    );

INSERT INTO test.b (
    id, a, b, c, d, [all], [level], e, t, create_at, gender,
    byteablob, textclob, exclude_me, casesensitive, country_id,
    rawbytea, json_like, doc, uuid, int16_t, int128_t, int256_t, bfloat16_t
) VALUES (
             1,
             123456.78,
             987654321,
             'Y',
             'NCHAR_VAL ',
             N'Тестовая строка NVARCHAR2',
             'Text VARCHAR2',
             123.45,
             '2026-06-18 15:01:37',
             '2026-06-18 15:01:37.694864 +00:00',
             1,
             0xDEADBEEF01020304,
             N'Текст внутри поля CLOB',
             999,
             N'РеГиСтР_СиМвОлОв',
             7,
             0xAABBCCDDEEFF00112233445566778899,
             '{"key": "just_string"}',
             '{"user": "Dmitrii", "role": "admin"}',
             '3e2e125a-b6c9-4f9b-9682-d21ec40564bc',
             32767,
             '170141183460469231731687303715884105727',
             '57896044618658097711785492504343953926634992332820282019728792003956564819967',
             123.45
         );

INSERT INTO test.b (id, exclude_me) VALUES (2, 900);
INSERT INTO test.b (id) VALUES (3);

create table test.t1(
    uuid UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
    id char(32),
    name nvarchar(256),
    constraint pk_t1 primary key (uuid, id)
);
