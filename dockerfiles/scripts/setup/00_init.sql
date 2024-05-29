alter session set container = ORCLPDB1;
create user test identified by test;
alter user test quota unlimited on users;
grant connect, resource to test;
create table test.table1 (
    id number(19,0),
    "LEVEL" varchar2(255),
    create_at timestamp(6) with time zone,
    update_at timestamp(6) with time zone,
    gender number(1,0) check (gender in (0,1)),
    byteablob blob,
    textclob clob,
    exclude_me int,
    "CaseSensitive" varchar2(20),
    country_id int,
    rawbytea raw(16),
    doc VARCHAR2(4000) check (doc is json),
    uuid char(36),
    clobjsonb clob,
    primary key (id)
);
create table test.countries (
    id int,
    name varchar2(256) not null,
    primary key (id)
);
insert into test.countries values (1, 'Brazil');
insert into test.countries values (2, 'China');
insert into test.countries values (3, 'Egypt');
insert into test.countries values (4, 'Ethiopia');
insert into test.countries values (5, 'India');
insert into test.countries values (6, 'Iran');
insert into test.countries values (7, 'Russia');
insert into test.countries values (8, 'South Africa');
insert into test.countries values (9, 'United Arab Emirates');
commit;
insert into test.table1
    (select
        rownum as id,
        'Hi, I''m using varchar2 to varchar' as "LEVEL",
        sysdate as create_at,
        systimestamp as update_at,
        mod(rownum, 2) as gender,
        utl_raw.cast_to_raw('Hi, I''m using CLOB to bytea') as byteablob,
        to_clob('Hi, I''m using CLOB to text') as textclob,
        null as exclude_me,
        null as "CaseSensitive",
        decode(round(dbms_random.value(0,9)),0,null,round(dbms_random.value(1,9))) as country_id,
        utl_raw.cast_to_raw('ABC' || rownum) as rawbytea,
        JSON_OBJECT('name' value 'Foo') as doc,
        '026cebda-a9f3-46da-80c9-d89bdd4841b3' as uuid,
        to_clob(JSON_OBJECT('name' value 'Foo')) as clobjsonb
    from dual connect by level < 1000000);
commit;
create table test."Table2" as
select id, "LEVEL", create_at, update_at, gender, byteablob, textclob, exclude_me, "CaseSensitive", country_id
from test.table1;
create table test.parted (
    id number(19,0) primary key,
    create_at timestamp(6) not null,
    name varchar2(1000))
  partition by range (create_at)
(
  partition parted_p0 values less than (to_date('01/01/2019', 'DD/MM/YYYY')),
  partition parted_p1 values less than (to_date('01/01/2020', 'DD/MM/YYYY')),
  partition parted_p2 values less than (to_date('01/01/2021', 'DD/MM/YYYY')),
  partition parted_p3 values less than (to_date('01/01/2022', 'DD/MM/YYYY')),
  partition parted_p4 values less than (to_date('01/01/2023', 'DD/MM/YYYY')),
  partition parted_p5 values less than (to_date('01/01/2024', 'DD/MM/YYYY')),
  partition parted_p6 values less than (to_date('01/01/2025', 'DD/MM/YYYY'))
);
create index parted_at_idx on test.parted (create_at);
insert into test.parted
    (select
        rownum as id,
        to_date('01/'||round(dbms_random.value(1,12))||'/'||round(dbms_random.value(2019,2024)), 'DD/MM/YYYY') as update_at,
        rpad('*', round(dbms_random.value(0,1000)),'*') as name
    from dual connect by level < 1000000);
commit;
