CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

CREATE OR REPLACE FUNCTION test.map_to_json (input map<text, text>)
    RETURNS NULL ON NULL INPUT
    RETURNS text
    LANGUAGE java
    AS $$
        if (input == null || input.isEmpty()) {
            return "{}";
        }
        StringBuilder json = new StringBuilder();
        json.append("{");
        boolean first = true;
        for (java.util.Map.Entry<String, String> entry : input.entrySet()) {
            if (!first) {
                json.append(",");
            }
            // Экранирование кавычек в ключах и значениях
            String key = entry.getKey().replace("\"", "\\\"");
            String value = entry.getValue().replace("\"", "\\\"");

            json.append("\"").append(key).append("\":\"").append(value).append("\"");
            first = false;
        }
        json.append("}");
        return json.toString();
    $$;

create table test.t1 (
    id int,
    a ascii,
    b bigint,
    c blob,
    d boolean,
    e date,
    f decimal,
    g double,
--    h duration,
    i float,
    j inet,
    k int,
    l smallint,
    m text,
    n time,
    o timestamp,
    p timeuuid,
    q tinyint,
    r uuid,
    s varchar,
    t varint,
    u text,
    mmap map<text, text>,
    primary key (id));
insert into test.t1
(id, a, b, c, d, e, f, g,
--h,
i, j, k, l, m, n, o, p, q, r, s, t, u, mmap)
values
(1, 'ascii', 9999999999, 0x0102030405060708090a0b0c0d0e0f101112131415161718, true, '2018-01-01', 123.456, 123.456,
--12h57m43s,
123.456, '127.0.0.1', 123, 123, 'text', '12:00:00', '2025-12-02T00:00:00.001+0000', 50554d6e-29bb-11e5-b345-feff819cdc9f,
123, 50554d6e-29bb-11e5-b345-feff819cdc9f, 'varchar', 123456789,
'{   "fromSchemaName": "test",
     "fromTableName": "t1",
     "toSchemaName": "public",
     "toTableName": "t1"   }',
 {'k1' : 'v1', 'k2' : 'v1', 'k3' : 'v2'});
