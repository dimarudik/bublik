package dev.bublik.clickhouse.constants;

public abstract class SQLConstants {
    public static final String SQL_ALL_COLUMNS = """
            SELECT name, type, position, is_in_partition_key, is_in_sorting_key, is_in_primary_key,
            is_in_sampling_key, default_expression 
            FROM system.columns 
            WHERE database = {db:String} AND table = {table:String}
            """;
}
