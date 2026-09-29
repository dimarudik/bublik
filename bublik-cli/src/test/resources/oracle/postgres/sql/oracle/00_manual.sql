alter session set container = freepdb1;
create user bublik identified by bublik;
alter user bublik quota unlimited on users;
grant create session to bublik;
grant create table to bublik;
grant select any table to bublik;
grant analyze any to bublik;
grant execute on SYS.DBMS_LOCK TO bublik;
grant execute on SYS.DBMS_LOCK TO test;
