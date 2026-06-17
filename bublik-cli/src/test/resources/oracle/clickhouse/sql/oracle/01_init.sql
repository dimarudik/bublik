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
    uuid char(36)
);
INSERT INTO test.b (
    id, a, b, c, d, "ALL", "LEVEL", e,
    t, create_at, gender, byteablob, textclob, exclude_me,
    "CaseSensitive", country_id, rawbytea, json_like, doc, uuid
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
    CURRENT_TIMESTAMP,
    SYSTIMESTAMP,
    1,
    TO_BLOB(HEXTORAW('DEADBEEF01020304')),
    TO_CLOB('Текст внутри поля CLOB'),
    999,
    'РеГиСтР_СиМвОлОв',
    7,
    HEXTORAW('AABBCCDDEEFF00112233445566778899'),
    '{"key": "just_string"}',
    '{"user": "Dmitrii", "role": "admin"}',
    '3e2e125a-b6c9-4f9b-9682-d21ec40564bc'
FROM dual;
INSERT INTO test.b (id) VALUES (2);
COMMIT;
