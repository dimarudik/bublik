alter session set container = freepdb1;
create table test.a (
    a int,
    b NUMBER(10),
    created timestamp
);
insert into test.a (a, b, created) values (1, 2.2, sysdate);
insert into test.a (a) values (2);
commit;
create table test.b (
    id int,
    a NUMBER(12,2),
    b NUMBER(10),
    c char,
    d nchar(10),
    "ALL" nvarchar2(255),
    "LEVEL" varchar2(255),
    e float(3),
    t timestamp,
    create_at timestamp(6) with time zone,
    gender number(1,0) check (gender in (0,1)),
    byteablob blob,
    textclob clob,
    exclude_me int,
    "CaseSensitive" varchar2(40),
    country_id int,
    rawbytea raw(16),
    json_like varchar2(4000),
    doc varchar2(4000) check (doc is json),
    uuid char(36),
    int16_t number(5),
    int128_t number,
    int256_t VARCHAR2(80),
    bfloat16_t number,
    dd date,
    dy date,
    dz date
);
INSERT INTO test.b (
    id,
    a,
    b,
    c,
    d,
    "ALL",
    "LEVEL",
    e,
    t,
    create_at,
    gender,
    byteablob,
    textclob,
    exclude_me,
    "CaseSensitive",
    country_id,
    rawbytea,
    json_like,
    doc,
    uuid,
    int16_t,
    int128_t,
    int256_t,
    bfloat16_t,
    dd,
    dy,
    dz
)
SELECT
    1,
    123456.78,
    987654321,
    'Y',
    'NCHAR_VAL ',
    'Тестовая строка NVARCHAR2',
    'Уровень доступа VARCHAR2',
    123.45,
    TO_TIMESTAMP('2026-06-18 15:01:37', 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP_TZ('2026-06-18 15:01:37.694864 +00:00', 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'),
--     CURRENT_TIMESTAMP,
--     SYSTIMESTAMP,
    1,
    TO_BLOB(HEXTORAW('DEADBEEF01020304')),
    TO_CLOB('Текст внутри поля CLOB'),
    999,
    'РеГиСтР_СиМвОлОв',
    7,
    HEXTORAW('AABBCCDDEEFF00112233445566778899'),
    '{"key": "just_string"}',
    '{"user": "Dmitrii", "role": "admin"}',
    '3e2e125a-b6c9-4f9b-9682-d21ec40564bc',
    32767,
    170141183460469231731687303715884105727,
    '57896044618658097711785492504343953926634992332820282019728792003956564819967',
    123.45,
    TO_DATE('2026-06-18', 'YYYY-MM-DD'),
    TO_DATE('3000-01-01', 'YYYY-MM-DD'),
        TO_DATE('3000-01-01', 'YYYY-MM-DD')
FROM dual;
INSERT INTO test.b (id, exclude_me) VALUES (2, 900);
INSERT INTO test.b (id) VALUES (3);
INSERT INTO test.b (id, t, a) VALUES (4, TO_TIMESTAMP('2026-06-18 15:01:38', 'YYYY-MM-DD HH24:MI:SS'), 200);
INSERT INTO test.b (id, t, a) VALUES (4, TO_TIMESTAMP('2026-06-18 15:01:37', 'YYYY-MM-DD HH24:MI:SS'), 100);
COMMIT;
