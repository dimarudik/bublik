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
    primary key (id)
);
create table test.table2 (
    id raw(16) not null,
    method number(3,0) check (method between 0 and 7),
    message varchar2(255),
    create_at timestamp(6) with time zone,
    version number(19,0),
    primary key (id)
);
