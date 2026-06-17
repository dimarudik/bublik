package dev.bublik.clickhouse.service;

import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;

import java.sql.ResultSet;

@FunctionalInterface
public interface ColumnTransfer {
    void transfer(ResultSet rs, String srcColumnName, RowBinaryFormatWriter writer, int chIndex) throws Exception;
}
