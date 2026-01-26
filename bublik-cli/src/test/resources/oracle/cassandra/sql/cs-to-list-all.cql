CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create table test.users (
    id int,
    attrs1 list<text>,
    attrs2 list<decimal>,
    kv1 map<decimal, text>,
    kv2 map<text, text>,
    s1 set<text>,
    s2 set<text>,
    primary key (id));
