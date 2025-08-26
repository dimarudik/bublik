create table `public/target` (
    id Uint64,
    uuid Uuid,
    boolean Bool,
    name Utf8,
    image Bytes,
    double Double,
    json Json,
    date Date,
    datetime Datetime,
    timestamp Timestamp,
    interval Interval,
    primary key(id)
);

create table `public/likes_all` (
    like_id Uint64,
    user_id Uint64,
    item_id Uint64,
    user_name bytes,
    email bytes,
    item_name bytes,
    description bytes,
    primary key(like_id)
);
