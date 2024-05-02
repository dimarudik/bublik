package org.example.service;

import org.example.model.OraTable;
import org.example.model.PGTable;
import org.example.model.Table;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface TableService {
    boolean exists(Connection connection) throws SQLException;
    String getFinalTableName(boolean withQuotes);
    String getFinalSchemaName();
    String getHintClause();
    String getTaskName();
    Map<String, String> getColumnToColumn(Connection connection) throws SQLException;

    static Class<? extends Table[]> getTableArrayClass(Connection connection) throws SQLException {
        if (connection.isWrapperFor(oracle.jdbc.OracleConnection.class)) {
            return OraTable[].class;
        } else if (connection.isWrapperFor(PGConnection.class)) {
            return PGTable[].class;
        }
        throw new SQLException("Unknown DataSource");
    }
}
