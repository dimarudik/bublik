create schema if not exists test;

CREATE TABLE test.b (
  id          BIGINT PRIMARY KEY,
  a           NUMERIC(12,2),
  b           BIGINT,
  c           CHAR(1),
  d           VARCHAR(10),
  "all"       VARCHAR(255),
  level       VARCHAR(255),
  e           REAL,
  t           TIMESTAMP,
  create_at   TIMESTAMP WITH TIME ZONE,
  gender      SMALLINT,
  byteablob   BYTEA,
  textclob    TEXT,
  exclude_me  BIGINT,
  casesensitive VARCHAR(40),
  country_id  BIGINT,
  rawbytea    BYTEA,
  json_like   TEXT,
  doc         TEXT,
  uuid        UUID,
  int16_t     SMALLINT,
  int128_t    NUMERIC,
  int256_t    NUMERIC,
  bfloat16_t  REAL
);

INSERT INTO test.b (
    id, a, b, c, d, "all", level, e, t, create_at, gender,
    byteablob, textclob, exclude_me, casesensitive, country_id,
    rawbytea, json_like, doc, uuid, int16_t, int128_t, int256_t, bfloat16_t
) VALUES (
    1,
    123456.78,
    987654321,
    'Y',
    'NCHAR_VAL ',
    'Тестовая строка NVARCHAR2',
    'Уровень доступа VARCHAR2',
    123.45,
    '2026-06-18 15:01:37'::timestamp,
    '2026-06-18 15:01:37.694864 +0000'::timestamptz,
    1,
    DECODE('DEADBEEF01020304', 'hex'),
    'Текст внутри поля CLOB',
    999,
    'РеГиСтР_СиМвОлОв',
    7,
    DECODE('AABBCCDDEEFF00112233445566778899', 'hex'),
    '{"key": "just_string"}',
    '{"user": "Dmitrii", "role": "admin"}',
    '3e2e125a-b6c9-4f9b-9682-d21ec40564bc'::uuid,
    32767,
    170141183460469231731687303715884105727,
    '57896044618658097711785492504343953926634992332820282019728792003956564819967',
    123.45
);

INSERT INTO test.b (id, exclude_me) VALUES (2, 900);
INSERT INTO test.b (id) VALUES (3);
