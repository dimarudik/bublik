alter session set container = freepdb1;
create user test identified by test;
alter user test quota unlimited on users;
grant connect, resource to test;
