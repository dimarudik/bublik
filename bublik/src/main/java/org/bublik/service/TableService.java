package org.bublik.service;

import org.bublik.model.OraTable;
import org.bublik.model.PGTable;
import org.bublik.model.Table;
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

    static Table getTable(Connection connection, String schemaName, String tableName) throws SQLException {
        if (connection.isWrapperFor(oracle.jdbc.OracleConnection.class)) {
            return new OraTable(schemaName, tableName);
        } else if (connection.isWrapperFor(PGConnection.class)) {
            return new PGTable(schemaName, tableName);
        }
        throw new SQLException("Unknown DataSource");
    }
}
