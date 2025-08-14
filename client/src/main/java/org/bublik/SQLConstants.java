package org.bublik;

public class SQLConstants {
    public static final String UPDATE_S50K_BY_ID =
            "update s50k set timestamp = now(), uuid = gen_random_uuid() where id = ?";
    public static final String UPDATE_S50K_BETWEEN_ID =
            "update s50k set timestamptz = now() where id between ? and ?";
    public static final String BATCH_INSERT_S50K =
            "insert into s50k (id, uuid, \"Primary\", boolean, " +
            "        int2, int4, int8, smallint, bigint, numeric, float8, " +
            "        date, timestamp, timestamptz, description, current_mood, time) " +
            "    select n as id, gen_random_uuid() as uuid, 'PostgreSQL ' || n as name, " +
            "        case when mod(n, 2) = 0 then false else true end as boolean, " +
            "        0 as int2, n as int4, n as int8, 10 as smallint, n as bigint, n / pi() as numeric, n / pi() as float8, " +
            "        current_date, current_timestamp, current_timestamp, " +
            "        rpad('PostgreSQL', 100, '*') as description, " +
            "        case " +
            "            when floor(random() * (3 + 1) + 0)::int = 1 then 'sad'::mood " +
            "            when floor(random() * (3 + 1) + 0)::int = 2 then 'ok'::mood " +
            "            when floor(random() * (3 + 1) + 0)::int = 2 then 'happy'::mood " +
            "            else null end as current_mood, " +
            "        now() as time " +
            "    from generate_series( (select max(id) + 1 from s50k) , (select max(id) + 1 from s50k) + ? ) as n";
    public static final String UPDATE_BY_ID =
            "update $schemaName.$tableName set touch_count = touch_count + 1, " +
            "item_id = (floor(random() * 100000 + 1)::int), " +
            "last_update = current_timestamp " +
            "where id = ? ";
    public static final String UPDATE_TAIL_BY_ID =
            "update $schemaName.$tableName set touch_count = touch_count + 1, " +
            "item_id = (floor(random() * 100000 + 1)::int), " +
            "last_update = current_timestamp " +
            "where id = (select max(id) - 1000 + ? from likes) ";
    public static final String UPDATE_BETWEEN_ID =
            "update $schemaName.$tableName set touch_count = 0 where id between ? and ?";
    public static final String BATCH_INSERT_LIKES =
            "insert into likes (id, user_id, item_id, r, last_update) " +
            "    select num as id, " +
            "       floor(random() * 100000 + 1)::int as user_id, " +
            "       floor(random() * 100000 + 1)::int as item_id,  " +
            "       rpad('Bublik is the best tool for migration ',100,'*') as r, " +
            "       current_timestamp as last_update " +
            "    from generate_series((select max(id) + 1 from likes) , (select max(id) + 1 from likes) + ? ) as num " +
            "on conflict (user_id, item_id) do nothing";
}
