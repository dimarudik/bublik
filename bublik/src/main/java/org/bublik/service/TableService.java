package org.bublik.service;

import org.bublik.model.*;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface TableService {
    boolean exists(Connection connection) throws SQLException;
    String getFinalTableName(boolean withQuotes);
    String getFinalSchemaName();
    String getHintClause();
    String getTaskName();
    List<Column> getAllColumns(Connection connection) throws SQLException;
    List<Column> getPrimaryKeyColumns(Connection connection) throws SQLException;
    List<Column> getImportedKeyColumns(Connection connection) throws SQLException;
    List<Index> getTableIndexes(Connection connection) throws SQLException;
    Map.Entry<Integer, List<TableOption>> getOptions(Connection connection) throws SQLException;
    boolean hasPrimaryKey();
    void createPrimaryKey(Connection connection);
    void createIndexes(Connection connection);
    Map<String, String> getColumnToColumn(Connection connection) throws SQLException;
    void createTable(Connection connection) throws SQLException;

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
        } else if (connection.isWrapperFor(tech.ydb.jdbc.YdbConnection.class)) {
            return new YDBTable(schemaName, tableName); // Assuming YDB uses similar table structure
        }
        throw new SQLException("Unknown DataSource");
    }
}
