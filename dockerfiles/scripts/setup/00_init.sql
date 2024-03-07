alter session set container = XEPDB1;
create user test identified by test;
alter user test quota unlimited on users;
grant connect, resource to test;
drop table test.users_tab;
drop sequence test.users_tab_seq;
drop table test.outbox_tab;
create table test.users_tab (
    id number(19,0),
    name varchar2(255),
    create_at timestamp(6) with time zone,
    update_at timestamp(6) with time zone,
    gender number(1,0) check (gender in (0,1)),
    primary key (id)
);
create sequence test.users_tab_seq start with 1 increment by 50;
create table test.outbox_tab (
    id raw(16) not null,
    method number(3,0) check (method between 0 and 7),
    message varchar2(255),
    create_at timestamp(6) with time zone,
    version number(19,0),
    primary key (id)
);
