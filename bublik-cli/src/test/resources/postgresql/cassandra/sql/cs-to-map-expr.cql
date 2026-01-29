CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

create table test.to_list (
    id int,
    attrs1 list<text>,
    attrs2 list<int>,
    attrs3 frozen<list<text>>,
    kv1 map<text, text>,
    kv2 map<int, text>,
    kv3 frozen<map<text, text>>,
    s1 set<text>,
    s2 frozen<set<text>>,
    s3 set<int>,
    primary key (id));
