package org.bublik.model;

import org.bublik.storage.Storage;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OraTable extends Table {
    public OraTable(){}
    public OraTable(String schemaName, String tableName) {
        super(schemaName, tableName);
    }

    @Override
    public boolean exists(Connection connection) throws SQLException {
        if (tableExistsCache().contains(this.getTableName())) {
            return true;
        }
        ResultSet tablesUpCase = connection.getMetaData().getTables(
                null,
                this.getSchemaName().toUpperCase(),
                getFinalTableName(false),
                null);
        if (!tablesUpCase.next()) {
            tablesUpCase.close();
            return false;
        }
        tablesUpCase.close();
        tableExistsCache().add(this.getTableName());
        return true;
    }

    @Override
    public String getFinalTableName(boolean withQuotes) {
        String tableName = withQuotes ? getTableName() : getWordWithoutQuotes(getTableName());
        return  isCaseSensitiveWord(getTableName()) ? tableName : getTableName().toUpperCase();
    }

    @Override
    public String getFinalSchemaName() {
        return getSchemaName().toUpperCase();
    }

    @Override
    public String getHintClause() {
        return "/*+ no_index(" + getFinalTableName(false) + ") */";
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
    public List<ForeignKey> getForeignKeys(Connection connection, Storage storage) throws SQLException {
        return List.of();
    }

    @Override
    public List<Index> getTableIndexes(Connection connection) throws SQLException {
        return List.of();
    }

    @Override
    public List<UniqueConstraint> getUniqueConstraints(Connection connection) throws SQLException {
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
    public void createIndexes(Connection connection) {

    }

    @Override
    public void createUniqueConstraints(Connection connection) {

    }

    @Override
    public void createForeignKeys(Connection connection) {

    }

    @Override
    public Map<String, String> getColumnToColumn(Connection connection) throws SQLException {
        Map<String, String> map = new HashMap<>();
        ResultSet columnsUpCase = connection.getMetaData().getColumns(
                null,
                getFinalSchemaName(),
                getFinalTableName(false),
                null);
        while (columnsUpCase.next()) {
            String columnName = columnsUpCase.getString(4);
            String finalColumnName = columnName.equals(columnName.toUpperCase()) ? columnName : "\"" + columnName + "\"";
            map.put(finalColumnName, finalColumnName);
        }
        return map;
    }

    @Override
    public void create(Connection connection) throws SQLException {

    }
}
