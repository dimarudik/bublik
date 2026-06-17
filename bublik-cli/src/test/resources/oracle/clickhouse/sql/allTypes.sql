CREATE TABLE IF NOT EXISTS a (
    a UInt32,
    b UInt64,
    created_at DateTime
) ENGINE = MergeTree()
    ORDER BY a;
