alter session set container = freepdb1;
create table test.to_ydb (
    id int,
    name varchar2(256));
insert into test.to_ydb
    (select
        rownum as id,
        rpad('*', round(dbms_random.value(0,200)),'*') as name
    from dual connect by level < 2000000);
commit;

