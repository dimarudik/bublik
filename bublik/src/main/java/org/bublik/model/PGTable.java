package org.bublik.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.bublik.constants.SQLConstants.SQL_INDEX_DEFINITION;

public class PGTable extends Table {
    private static final Logger log = LoggerFactory.getLogger(PGTable.class);

    public PGTable(){}
    public PGTable(String schemaName, String tableName) {
        super(schemaName, tableName);
    }

    @Override
    public boolean exists(Connection connection) throws SQLException {
/*
        if (tableExistsCache().contains(getFinalTableName(false))) {
            return true;
        }
*/
        ResultSet tablesLowCase = connection.getMetaData().getTables(
                null,
                getFinalSchemaName(false),
                getFinalTableName(false),
                null);
        if (!tablesLowCase.next()) {
            tablesLowCase.close();
            return false;
        }
        tablesLowCase.close();
        tableExistsCache().add(getFinalTableName(false));
        return true;
    }

    @Override
    public String getFinalTableName(boolean withQuotes) {
        String tableName = withQuotes ? getTableName() : getWordWithoutQuotes(getTableName());
        return  isCaseSensitiveWord(getTableName()) ? tableName : getTableName().toLowerCase();
    }

    @Override
    public String getFinalSchemaName() {
        return getSchemaName().toLowerCase();
    }

    public String getFinalSchemaName(boolean withQuotes) {
        String schemaName = withQuotes ? getSchemaName() : getWordWithoutQuotes(getSchemaName());
        return  isCaseSensitiveWord(getSchemaName()) ? schemaName : getSchemaName().toLowerCase();
    }

    @Override
    public String getHintClause() {
        return " ";
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
            columns.add(new Column(
                    ordinalPosition,
                    columnName,
                    columnType,
                    dataType,
                    nullable,
                    columnDefault,
                    isAutoIncrement,
                    isGenerated
            ));
            log.info("Column: {}, Data Type: {}, Column Type: {}, Nullable: {}, Default: {}, Auto Increment: {}, Generated: {}, Ordinal Position: {}",
                    columnName, dataType, columnType, nullable, columnDefault, isAutoIncrement, isGenerated, ordinalPosition);
        }
        columns.sort(Column::compareTo);
        return columns;
    }

    @Override
    public List<Column> getPrimaryKeyColumns(Connection connection) throws SQLException {
        List<Column> pkColumns = new ArrayList<>();
        ResultSet rs = connection.getMetaData().getPrimaryKeys(
                null, getFinalSchemaName(), getFinalTableName(false));
        while (rs.next()) {
            Integer keySeq = rs.getInt("KEY_SEQ");
            String pkName = rs.getString("PK_NAME");
            String columnName = rs.getString("COLUMN_NAME");
            pkColumns.add(new Column(keySeq, columnName, null, null, null, null, null, null));
        }
        rs.close();
        pkColumns.sort(Column::compareTo);
        return pkColumns;
    }

    @Override
    public List<Index> getIndexes(Connection connection) throws SQLException {
        Map<String, Index> indexes = new HashMap<>();
        try {
            ResultSet rs = connection.getMetaData().getIndexInfo(
                    null,
                    getFinalSchemaName(),
                    getFinalTableName(false),
                    false,
                    false
            );
            while (rs.next()) {
                boolean nonUnique = rs.getBoolean("NON_UNIQUE");
                String indexName = rs.getString("INDEX_NAME");
                short ordinalPosition = rs.getShort("ORDINAL_POSITION");
                String columnName = rs.getString("COLUMN_NAME");
                String ascOrDesc = rs.getString("ASC_OR_DESC");
                String filterCondition = rs.getString("FILTER_CONDITION");
                PreparedStatement ps = connection.prepareStatement(SQL_INDEX_DEFINITION);
                ps.setString(1, getFinalSchemaName(true));
                ps.setString(2, getFinalTableName(true));
                ps.setString(3, indexName);
                ResultSet set = ps.executeQuery();
                String indexDefinition = "";
                while (set.next()) {
                    indexDefinition = set.getString("indexdef");
                }
                Column column = new Column(
                        null,
                        columnName,
                        null, // columnType is not used here
                        null, // dataType is not used here
                        null, // nullable is not used here
                        null, // defaultValue is not used here
                        null, // isAutoIncrement is not used here
                        null  // isGenerated is not used here
                );
                if (indexes.containsKey(indexName)) {
                    Index existingIndex = indexes.get(indexName);
                    Map<Short, Column> map = existingIndex.getColumns();
                    map.put(ordinalPosition, column);
                    indexes.put(indexName, existingIndex);
                } else {
                    indexes.put(indexName, new Index(
                            this,
                            indexName,
                            new TreeMap<>() {{
                                put(ordinalPosition, column);
                            }},
                            nonUnique,
                            ascOrDesc,
                            filterCondition,
                            indexDefinition
                    ));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        indexes.forEach((s, index) ->
            log.info("Index: {}, Non-Unique: {}, Asc/Desc: {}, Filter Condition: {}, Definition: {}",
                    index.getIndexName(), index.isNonUnique(), index.getAscOrDesc(),
                    index.getFilterCondition(), index.getIndexDef())
        );
        return indexes.values().stream().toList();
    }

    @Override
    public boolean hasPrimaryKey() {
        return !getPkColumns().isEmpty();
    }

    @Override
    public void createPrimaryKey(Connection connection) {
        StringBuilder pkQuery = new StringBuilder("ALTER TABLE ");
        pkQuery.append(getFinalSchemaName(true)).append(".").append(getFinalTableName(true))
                .append(" ADD PRIMARY KEY (");
        for (int i = 0; i < getPkColumns().size(); i++) {
            Column column = getPkColumns().get(i);
            pkQuery.append(column.getColumnName());
            if (i < getPkColumns().size() - 1) {
                pkQuery.append(", ");
            }
        }
        pkQuery.append(");");
        log.info("Creating primary key for table {}.{}: {}", getSchemaName(), getTableName(), pkQuery);
        try {
            connection.createStatement().execute(pkQuery.toString());
            connection.commit();
        } catch (SQLException e) {
            log.error("Failed to create primary key for table {}.{}: {}", getSchemaName(), getTableName(), e.getMessage());
        }
    }

    @Override
    public void createIndex(Connection connection) {

    }

    @Override
    public Map<String, String> getColumnToColumn(Connection connection) throws SQLException {
        Map<String, String> map = new HashMap<>();
        ResultSet columnsLowCase = connection.getMetaData().getColumns(
                null,
                getFinalSchemaName(),
                getFinalTableName(false),
                null);
        while (columnsLowCase.next()) {
            String columnName = columnsLowCase.getString(4);
            String finalColumnName = columnName.equals(columnName.toLowerCase()) ? columnName : "\"" + columnName + "\"";
            map.put(finalColumnName, finalColumnName);
        }
        return map;
    }
}
