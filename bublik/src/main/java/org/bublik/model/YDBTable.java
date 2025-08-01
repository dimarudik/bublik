package org.bublik.model;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class YDBTable extends Table {
    public YDBTable(){}
    public YDBTable(String schemaName, String tableName) {
        super(schemaName, tableName);
    }

    @Override
    public boolean exists(Connection connection) throws SQLException {
        if (tableExistsCache().contains(this.getTableName())) {
            return true;
        }
        ResultSet tables = connection.getMetaData().getTables(
                null,
                getSchemaName(),
                getTableName(),
                null);
        if (!tables.next()) {
            tables.close();
            return false;
        }
        tables.close();
        tableExistsCache().add(getTableName());
        return true;
/*
        YdbConnection ydbConnection = connection.unwrap(YdbConnection.class);
        ydbConnection.getMetaData().getTables()
*/
    }

    @Override
    public String getFinalTableName(boolean withQuotes) {
        return getTableName();
    }

    @Override
    public String getFinalSchemaName() {
        return getSchemaName();
    }

    @Override
    public String getHintClause() {
        return "";
    }

    @Override
    public List<Column> getAllColumns(Connection connection) throws SQLException {
        return List.of();
    }

    @Override
    public List<Column> getPrimaryKeyColumns(Connection connection) throws SQLException {
        return List.of();
    }

    @Override
    public List<Column> getImportedKeyColumns(Connection connection) throws SQLException {
        return List.of();
    }

    @Override
    public List<Index> getIndexes(Connection connection) throws SQLException {
        return List.of();
    }

    @Override
    public Map.Entry<Integer, List<TableOption>> getOptions(Connection connection) throws SQLException {
        return null;
    }

    @Override
    public boolean hasPrimaryKey() {
        return false;
    }

    @Override
    public void createPrimaryKey(Connection connection) {

    }

    @Override
    public void createIndex(Connection connection) {

    }

    @Override
    public Map<String, String> getColumnToColumn(Connection connection) throws SQLException {
        return Map.of();
    }

    @Override
    public void createTableIfNotExists(Connection connection) throws SQLException {

    }
}
