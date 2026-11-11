alter session set container = freepdb1;
grant execute on SYS.DBMS_LOCK TO test;
grant select_catalog_role to test;
