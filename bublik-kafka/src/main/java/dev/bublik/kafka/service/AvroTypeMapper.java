package dev.bublik.kafka.service;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface AvroTypeMapper {
    Object getValue(ResultSet rs, String columnName) throws SQLException;
}
