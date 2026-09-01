CREATE TABLE IF NOT EXISTS a (
    a UInt32,
    b Nullable(UInt64),
    created_at Nullable(DateTime)
) ENGINE = MergeTree()
    ORDER BY a;

CREATE TABLE IF NOT EXISTS b (
    ID UInt32,
    A Nullable(Decimal(12, 2)),
    B Nullable(UInt64),
    C Nullable(FixedString(1)),
    D Nullable(String),
    ALL Nullable(String),
    LEVEL Nullable(String),
    E Nullable(Float32),
    T Nullable(DateTime),
    CREATE_AT Nullable(DateTime64(6, 'UTC')),
    GENDER Nullable(UInt8),
    BYTEABLOB Nullable(String),
    TEXTCLOB Nullable(String),
    EXCLUDE_ME Nullable(Int32),
    CaseSensitive Nullable(String),
    COUNTRY_ID Nullable(Int32),
    RAWBYTEA Nullable(String),
    JSON_LIKE Nullable(String),
    DOC Nullable(String),
    UUID Nullable(UUID),
    INT16_T Nullable(Int16),
    INT128_T Nullable(Int128),
    INT256_T Nullable(Int256),
    BFLOAT16_T Nullable(BFloat16),
    DD Nullable(Date),
    DY Nullable(DateTime64(0)),
    DZ Nullable(Date32)
    ) ENGINE = MergeTree()
    ORDER BY ID;

CREATE TABLE IF NOT EXISTS c (
    ID UInt32,
    A Nullable(Decimal(12, 2)),
    B Nullable(UInt64),
    C Nullable(FixedString(1)),
    D Nullable(String),
    ALL Nullable(String),
    LEVEL Nullable(String),
    E Nullable(Float32),
    T Nullable(DateTime),
    CREATE_AT Nullable(DateTime64(6, 'UTC')),
    GENDER Nullable(UInt8),
    BYTEABLOB Nullable(String),
    TEXTCLOB Nullable(String),
    EXCLUDE_ME Nullable(Int32),
    CaseSensitive Nullable(String),
    COUNTRY_ID Nullable(Int32),
    RAWBYTEA Nullable(String),
    JSON_LIKE Nullable(String),
    DOC Nullable(String),
    UUID Nullable(UUID),
    INT16_T Nullable(Int16),
    INT128_T Nullable(Int128),
    INT256_T Nullable(Int256)
    ) ENGINE = MergeTree()
    ORDER BY ID;

CREATE TABLE IF NOT EXISTS d (
    ID UInt32,
    A Nullable(Decimal(12, 2)),
    B Nullable(UInt64),
    C Nullable(FixedString(1)),
    D Nullable(String),
    ALL Nullable(String),
    LEVEL Nullable(String),
    E Nullable(Float32),
    T DateTime,
    CREATE_AT Nullable(DateTime64(6, 'UTC')),
    GENDER Nullable(UInt8),
    BYTEABLOB Nullable(String),
    TEXTCLOB Nullable(String),
    EXCLUDE_ME Nullable(Int32),
    CaseSensitive Nullable(String),
    COUNTRY_ID Nullable(Int32),
    RAWBYTEA Nullable(String),
    JSON_LIKE Nullable(String),
    DOC Nullable(String),
    UUID Nullable(UUID),
    INT16_T Nullable(Int16),
    INT128_T Nullable(Int128),
    INT256_T Nullable(Int256)
    ) ENGINE = ReplacingMergeTree(T)
    ORDER BY ID;

CREATE TABLE IF NOT EXISTS MANUAL_CHUNKS (
    ID UInt32,
    NAME Nullable(String)
) ENGINE = MergeTree()
ORDER BY ID;
