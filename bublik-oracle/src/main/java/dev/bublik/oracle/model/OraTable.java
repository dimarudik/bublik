package dev.bublik.oracle.model;

import dev.bublik.core.model.*;
import dev.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OraTable<S extends Connection> extends Table<S> {
    private static final Logger log = LoggerFactory.getLogger(OraTable.class);

//    public OraTable(){}
    public OraTable(String schemaName, String tableName) {
        super(schemaName, tableName);
    }

    @Override
    public boolean exists(Connection connection) throws SQLException {
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
//        tableExistsCache().add(this.getTableName());
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
//            String columnDefault = rs.getString("COLUMN_DEF");
            String isAutoIncrement = rs.getString("IS_AUTOINCREMENT");
            String isGenerated = rs.getString("IS_GENERATEDCOLUMN");
            int decimalDigits = rs.getInt("DECIMAL_DIGITS");
            String remark = rs.getString("REMARKS");
            int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
            columns.add(new Column(
                    ordinalPosition,
                    isCaseSensitiveWord(columnName) || isReservedWord(columnName) ? "\"" + columnName + "\"" : columnName,
                    columnType,
                    dataType,
                    nullable,
                    null,
//                    columnDefault,
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
        log.info("Table {}.{} not exists in target database. Please create it manually.", getFinalSchemaName(), getFinalTableName(false));
    }

    @Override
    public boolean enrichTable(S session) throws SQLException {
        if (exists(session)) {
            setColumns(getAllColumns(session));
            setPkColumns(getPrimaryKeyColumns(session));
            return true;
        }
        return false;
    }
}
