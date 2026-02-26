package dev.bublik.core.constants;

public abstract class CLassConstants {
    public static final String ORACLE_STORAGE_CLASS_NAME = "dev.bublik.oracle.storage.JDBCOracleStorage";
    public static final String POSTGRES_STORAGE_CLASS_NAME = "dev.bublik.postgres.storage.JDBCPostgreSQLStorage";
    public static final String YDB_STORAGE_CLASS_NAME = "dev.bublik.ydb.storage.JDBCYDBStorage";
    public static final String DEFAULT_FETCH_WHERE_CLAUSE = "1 = 1";
//    public static final String CASSANDRA_STORAGE_CLASS_NAME = "org.bublik.cassandra.storage.CSPoolStorage";
}
