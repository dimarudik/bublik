package dev.bublik.clickhouse.service;

import java.sql.ResultSet;

@FunctionalInterface
public interface ColumnTransfer {
    void transfer(ResultSet rs, int jdbcIndex, com.google.common.io.LittleEndianDataOutputStream out) throws Exception;
}
