package dev.bublik.mssql.model;

import dev.bublik.core.model.*;
import dev.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static dev.bublik.mssql.constants.SQLConstants.SQL_CLUSTERING_KEY;

public class MSSQLTable<S extends Connection> extends Table<S> {
    private static final Logger log = LoggerFactory.getLogger(MSSQLTable.class);
    private List<Column> clusteringKey;

    public MSSQLTable(String schemaName, String tableName, List<Column> clusteringKey) {
        super(schemaName, tableName);
        this.clusteringKey = clusteringKey;
    }

    public List<Column> getClusteringKey() {
        return clusteringKey;
    }

    public void setClusteringKey(List<Column> clusteringKey) {
        this.clusteringKey = clusteringKey;
    }

    public List<Column> getClusteringKeyColumns(S connection) throws SQLException {
        Set<Column> columns = new TreeSet<>();
        PreparedStatement ps = connection.prepareStatement(SQL_CLUSTERING_KEY);
        ps.setString(1, getSchemaName() + "." + getTableName());
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            columns.add(new Column(
                    rs.getInt("key_ordinal"),
                    rs.getString("column_name"),
                    rs.getString("column_type"),
                    rs.getInt("is_nullable"),
                    rs.getInt("max_length"),
                    rs.getString("is_descending_key")));
        }
        return new ArrayList<>(columns);
    }

    @Override
    public boolean exists(Connection connection) throws SQLException {
        ResultSet tablesLowCase = connection.getMetaData().getTables(
                null,
                getFinalSchemaName(),
                getFinalTableName(false),
                null);
        if (!tablesLowCase.next()) {
            tablesLowCase.close();
            return false;
        }
        tablesLowCase.close();
//        tableExistsCache().add(getFinalTableName(false));
        return true;
    }

    @Override
    public String getFinalTableName(boolean withQuotes) {
        String tableName = withQuotes ? getTableName() : getWordWithoutQuotes(getTableName());
        return  isCaseSensitiveWord(getTableName()) ? tableName : getTableName().toLowerCase();
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
    public List<Column> getAllColumns(S connection) throws SQLException {
        List<Column> columns = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM " + getSchemaName() + "." + getTableName() +  " WHERE 1 = 0 ")) {
            ResultSetMetaData rsmd = ps.getMetaData();

            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                int ordinalPosition = i;
                String columnName = rsmd.getColumnName(i);
                String columnType = rsmd.getColumnTypeName(i);
                int dataType = rsmd.getColumnType(i); // Код из java.sql.Types
                int nullable = rsmd.isNullable(i);    // 0: No, 1: Yes, 2: Unknown
                int decimalDigits = rsmd.getScale(i);
                int charOctetLength = rsmd.getColumnDisplaySize(i);
                boolean isAuto = rsmd.isAutoIncrement(i);
                String isAutoIncrement = isAuto ? "YES" : "NO";
                columns.add(new Column(
                        ordinalPosition,
                        columnName,
                        columnType,
                        dataType,
                        nullable,
                        null,
                        isAutoIncrement,
                        null,
                        decimalDigits,
                        null,
                        charOctetLength,
                        null,
                        false,
                        false,
                        false
                ));
            }
        }

/*
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
            log.info("{}", columnName);
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
*/
        columns.sort(Column::compareTo);
        return columns;
    }

    @Override
    public List<Column> getPrimaryKeyColumns(Connection connection) throws SQLException {
        return List.of();
    }

    @Override
    public List<ForeignKey> getForeignKeys(Connection connection, Storage storage, Table targetTable) throws SQLException {
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
            setClusteringKey(getClusteringKeyColumns(session));
            return true;
        }
        return false;
    }
}
