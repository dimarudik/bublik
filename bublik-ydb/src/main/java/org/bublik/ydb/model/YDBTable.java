package org.bublik.ydb.model;

import org.bublik.core.model.*;
import org.bublik.core.storage.Storage;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class YDBTable<S extends Connection> extends Table<S> {
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
        List<Column> columns = new ArrayList<>();
        ResultSet rs = connection.getMetaData().getColumns(
                null,
                getFinalSchemaName(),
                getFinalTableName(false),
                null);
        while (rs.next()) {
            int ordinalPosition = rs.getInt("ORDINAL_POSITION");
            String columnName = rs.getString("COLUMN_NAME");
            String columnType = rs.getString("TYPE_NAME");
            Integer dataType = rs.getInt("DATA_TYPE");
            int nullable = rs.getInt("NULLABLE");
            String columnDefault = rs.getString("COLUMN_DEF");
            String isAutoIncrement = rs.getString("IS_AUTOINCREMENT");
            String isGenerated = rs.getString("IS_GENERATEDCOLUMN");
            int decimalDigits = rs.getInt("DECIMAL_DIGITS");
            String remark = rs.getString("REMARKS");
            int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
            columns.add(new Column(
                    ordinalPosition,
                    isCaseSensitiveWord(columnName) || isReservedWord(columnName) ? "\"" + columnName + "\"" : columnName,
                    columnType.equals("bigserial") ? "bigint" : columnType,
                    dataType,
                    nullable,
                    columnDefault,
                    isAutoIncrement,
                    isGenerated,
                    decimalDigits,
                    remark,
                    charOctetLength,
                    null,
                    false,
                    false,
                    false
            ));
        }
        columns.sort(Column::compareTo);
        return columns;
    }

    @Override
    public List<Column> getPrimaryKeyColumns(Connection connection) throws SQLException {
        return List.of();
    }

    @Override
    public List<ForeignKey> getForeignKeys(Connection connection, Storage storage, Table table) throws SQLException {
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
        return Map.of();
    }

    @Override
    public void create(Connection connection) throws SQLException {

    }

    @Override
    public boolean enrichTable(S session) throws SQLException {
        if (exists(session)) {
            setColumns(getAllColumns(session));
            return true;
        }
        return false;
    }
}
