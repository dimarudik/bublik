alter session set container = XEPDB1;
create user test identified by test;
alter user test quota unlimited on users;
grant connect, resource to test;
create table test.table1 (
    id number(19,0),
    name varchar2(255),
    create_at timestamp(6) with time zone,
    update_at timestamp(6) with time zone,
    gender number(1,0) check (gender in (0,1)),
    byteablob blob,
    textclob clob,
    exclude_me int,
    primary key (id)
);
insert into test.table1
    (select
        rownum as id,
        'Hi, I''m using varchar2 to varchar' as name,
        sysdate as create_at,
        systimestamp as update_at,
        mod(rownum, 2) as gender,
        utl_raw.cast_to_raw('Hi, I''m using CLOB to bytea') as byteablob,
        to_clob('Hi, I''m using CLOB to text') as textclob,
        null as exclude_me
    from dual connect by level < 100000);
commit;
