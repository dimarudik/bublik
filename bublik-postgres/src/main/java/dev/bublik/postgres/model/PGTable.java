package dev.bublik.postgres.model;

import dev.bublik.core.model.*;
import dev.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static dev.bublik.postgres.constants.SQLConstants.*;

public class PGTable<S extends Connection> extends Table<S> {
    private static final Logger log = LoggerFactory.getLogger(PGTable.class);

    public PGTable(String schemaName, String tableName) {
        super(schemaName, tableName);
    }

    @Override
    public boolean exists(Connection connection) throws SQLException {
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
    public List<ForeignKey> getForeignKeys(Connection connection, Storage storage, Table targetTable) throws SQLException {
        Map<String, ForeignKey> foreignKeys = new HashMap<>();
        ResultSet rs = connection.getMetaData().getImportedKeys(
                null,
                getFinalSchemaName(),
                getFinalTableName(false));
        while (rs.next()) {
            String pkSchemaName = rs.getString("PKTABLE_SCHEM");
            String pkTableName = rs.getString("PKTABLE_NAME");
            String pkColumnName = rs.getString("PKCOLUMN_NAME");
            short ordinalPosition = rs.getShort("KEY_SEQ");
            String fkSchemaName = rs.getString("FKTABLE_SCHEM");
            String fkTableName = rs.getString("FKTABLE_NAME");
            String fkColumnName = rs.getString("FKCOLUMN_NAME");
            String fkName = rs.getString("FK_NAME");
            String pkName = rs.getString("PK_NAME");
            String updateRule = rs.getString("UPDATE_RULE");
            String deleteRule = rs.getString("DELETE_RULE");
            short deferrability = rs.getShort("DEFERRABILITY");
            if (foreignKeys.containsKey(fkName)) {
                ForeignKey existingForeignKey = foreignKeys.get(fkName);
                existingForeignKey.getPkColumns().add(new Column(
                        (int) ordinalPosition,
                        pkColumnName,
                        null, // columnType is not used here
                        null, // dataType is not used here
                        null, // nullable is not used here
                        null, // defaultValue is not used here
                        null, // isAutoIncrement is not used here
                        null, // isGenerated is not used here
                        0, // decimalDigits is not used here
                        null, // columnComment is not used here
                        0, // charOctetLength is not used here
                        null,
                        false,
                        false,
                        false// ascOrDesc is not used here
                ));
                existingForeignKey.getFkColumns().add(new Column(
                        (int) ordinalPosition,
                        fkColumnName,
                        null, // columnType is not used here
                        null, // dataType is not used here
                        null, // nullable is not used here
                        null, // defaultValue is not used here
                        null, // isAutoIncrement is not used here
                        null, // isGenerated is not used here
                        0, // decimalDigits is not used here
                        null, // columnComment is not used here
                        0, // charOctetLength is not used here
                        null,
                        false,
                        false,
                        false// ascOrDesc is not used here
                ));
            } else {
                Table fkTable = new PGTable(fkSchemaName, fkTableName);
                Table pkTable = storage.getSourceTableByTargetTable(new PGTable(pkSchemaName, pkTableName));
                if (fkTable.equals(targetTable) && pkTable != null) {
                    ForeignKey foreignKey = new ForeignKey(
                            targetTable,
                            new ArrayList<>() {{
                                add(new Column(
                                        (int) ordinalPosition,
                                        fkColumnName,
                                        null, // columnType is not used here
                                        null, // dataType is not used here
                                        null, // nullable is not used here
                                        null, // defaultValue is not used here
                                        null, // isAutoIncrement is not used here
                                        null, // isGenerated is not used here
                                        0, // decimalDigits is not used here
                                        null, // columnComment is not used here
                                        0, // charOctetLength is not used here
                                        null,
                                        false,
                                        false,
                                        false// ascOrDesc is not used here
                                ));
                            }},
                            pkTable,
                            new ArrayList<>() {{
                                add(new Column(
                                        (int) ordinalPosition,
                                        pkColumnName,
                                        null, // columnType is not used here
                                        null, // dataType is not used here
                                        null, // nullable is not used here
                                        null, // defaultValue is not used here
                                        null, // isAutoIncrement is not used here
                                        null, // isGenerated is not used here
                                        0, // decimalDigits is not used here
                                        null, // columnComment is not used here
                                        0, // charOctetLength is not used here
                                        null,
                                        false,
                                        false,
                                        false// ascOrDesc is not used here
                                ));
                            }},
                            pkName,
                            fkName,
                            updateRule,
                            deleteRule,
                            deferrability);
                    foreignKeys.put(fkName, foreignKey);
                }
            }
/*
            log.info("Foreign Key of {}: PK Table: {}.{}, PK Column: {}, Ordinal Position: {}, " +
                            "FK Table: {}.{}, FK Column: {}, FK Name: {}, PK Name: {}, " +
                            "Update Rule: {}, Delete Rule: {}, Deferrability: {}",
                    getFinalTableName(false), pkSchemaName, pkTableName, pkColumnName, ordinalPosition,
                    fkSchemaName, fkTableName, fkColumnName, fkName, pkName,
                    updateRule, deleteRule, deferrability);
*/
        }
        foreignKeys.values().forEach(fk -> log.info("{}.{} : {}.{} {}.{}",
                getSchemaName(), getTableName(),
                fk.getPkTable().getSchemaName(), fk.getPkTable().getTableName(),
                fk.getFkTable().getSchemaName(), fk.getFkTable().getTableName()));

        return new ArrayList<>(foreignKeys.values());
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
//            тут
//            System.out.println(columnName + "   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
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
        List<Column> pkColumns = new ArrayList<>();
        ResultSet rs = connection.getMetaData().getPrimaryKeys(
                null, getFinalSchemaName(), getFinalTableName(false));
        while (rs.next()) {
            Integer keySeq = rs.getInt("KEY_SEQ");
            String pkName = rs.getString("PK_NAME");
            String columnName = rs.getString("COLUMN_NAME");
            pkColumns.add(new Column(
                    keySeq,
                    columnName,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    0,
                    null,
                    0,
                    null,
                    false,
                    false,
                    false)
            );
        }
        rs.close();
        pkColumns.sort(Column::compareTo);
        return pkColumns;
    }

    @Override
    public List<Index> getTableIndexes(Connection connection) throws SQLException {
        Map<Integer, Index> nonUniqueIndexesBasicColumns = getTableIndexes(connection, false, true);
        Map<Integer, Index> nonUniqueIndexesIncludeColumns = getTableIndexes(connection, false, false);
        Map<Integer, Index> UniqueIndexesBasicColumns = getTableIndexes(connection, true, true);
        Map<Integer, Index> UniqueIndexesIncludeColumns = getTableIndexes(connection, true, false);
        Map<Integer, Index> indexes = combineIndexes(
                combineIndexes(nonUniqueIndexesBasicColumns, nonUniqueIndexesIncludeColumns),
                combineIndexes(UniqueIndexesBasicColumns, UniqueIndexesIncludeColumns)
        );
        return new ArrayList<>(indexes.values());
    }

    @Override
    public List<UniqueConstraint> getUniqueConstraints(Connection connection) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(SQL_PG_UNIQUE_CONSTRAINTS);
        ps.setString(1, getFinalSchemaName(true));
        ps.setString(2, getFinalTableName(true));
        ResultSet rs = ps.executeQuery();
        Map<String, UniqueConstraint> uniqueConstraintMap = new HashMap<>();
        while (rs.next()) {
            String constraintName = rs.getString("conname");
            boolean nullsNotDistinct = rs.getBoolean("indnullsnotdistinct");
            int ordinalPosition = rs.getInt("pos");
            String columnName = rs.getString("attname");
            if (uniqueConstraintMap.containsKey(constraintName)) {
                UniqueConstraint existingConstraint = uniqueConstraintMap.get(constraintName);
                existingConstraint.getColumns().put((short) ordinalPosition, new Column(
                        ordinalPosition,
                        columnName/*,
                        null, // columnType is not used here
                        null, // dataType is not used here
                        null, // nullable is not used here
                        null, // defaultValue is not used here
                        null, // isAutoIncrement is not used here
                        null, // isGenerated is not used here
                        0, // decimalDigits is not used here
                        null, // columnComment is not used here
                        0, // charOctetLength is not used here
                        null,
                        false,
                        false,
                        false// ascOrDesc is not used here*/
                ));
            } else {
                uniqueConstraintMap.put(constraintName, new UniqueConstraint(
                        constraintName,
                        new TreeMap<>() {{
                            put((short) ordinalPosition, new Column(
                                    ordinalPosition,
                                    columnName/*,
                                    null, // columnType is not used here
                                    null, // dataType is not used here
                                    null, // nullable is not used here
                                    null, // defaultValue is not used here
                                    null, // isAutoIncrement is not used here
                                    null, // isGenerated is not used here
                                    0, // decimalDigits is not used here
                                    null, // columnComment is not used here
                                    0, // charOctetLength is not used here
                                    null,
                                    false,
                                    false,
                                    false// ascOrDesc is not used here*/
                            ));
                        }},
                        nullsNotDistinct));
            }
        }
        rs.close();
        ps.close();
        return new ArrayList<>(uniqueConstraintMap.values());
    }

    private Map<Integer, Index> combineIndexes(
            Map<Integer, Index> basicIndexes,
            Map<Integer, Index> includeIndexes) {
        Map<Integer, Index> combinedIndexes = new HashMap<>(basicIndexes);
        for (Map.Entry<Integer, Index> entry : includeIndexes.entrySet()) {
            int id = entry.getKey();
            Index index = entry.getValue();
            if (combinedIndexes.containsKey(id)) {
                Index existingIndex = combinedIndexes.get(id);
                existingIndex.getIncludeColumns().putAll(index.getIncludeColumns());
                combinedIndexes.put(id, existingIndex);
            } else {
                combinedIndexes.put(id, index);
            }
        }
        return combinedIndexes;
    }

    private Map<Integer, Index> getTableIndexes(Connection connection, boolean isUnique, boolean basic) throws SQLException {
        Map<Integer, Index> indexes = new HashMap<>();
        PreparedStatement ps = connection.prepareStatement(
                basic ? SQL_PG_INDEX_BASIC_COLUMNS : SQL_PG_INDEX_INCLUDE_COLUMNS);
        ps.setString(1, getFinalSchemaName(true));
        ps.setString(2, getFinalTableName(true));
        ps.setBoolean(3, isUnique);
        ResultSet rs = ps.executeQuery();
        if(rs.isBeforeFirst()) {
            while (rs.next()) {
                // Common index entities
                int id = rs.getInt("id");
                String indexName = rs.getString("relname");
                boolean isUniq = rs.getBoolean("uniq");
                String filterCondition = rs.getString("filter");
                String indexDef = rs.getString("indexdef");
                // Columns in the index
                Map<Short, Column> columns = new TreeMap<>();
                int ordinalPosition = rs.getInt("pos");
                String columnName = rs.getString("name");
                String ascOrDesc = rs.getString("ascdesc");
                Column column = new Column(
                        ordinalPosition,
                        columnName,
                        null, // columnType is not used here
                        null, // dataType is not used here
                        null, // nullable is not used here
                        null, // defaultValue is not used here
                        null, // isAutoIncrement is not used here
                        null, // isGenerated is not used here
                        0, // decimalDigits is not used here
                        null, // columnComment is not used here
                        0, // charOctetLength is not used here
                        ascOrDesc,
                        false,
                        false,
                        false
                );
                if (indexes.containsKey(id)) {
                    Index existingIndex = indexes.get(id);
                    if (basic) {
                        Map<Short, Column> map = existingIndex.getColumns();
                        map.put((short) ordinalPosition, column);
                        indexes.put(id, existingIndex);
                    } else {
                        Map<Short, Column> map = existingIndex.getIncludeColumns();
                        map.put((short) ordinalPosition, column);
                        indexes.put(id, existingIndex);
                    }
                } else {
                    Map<Short, Column> columnBasicMap = new TreeMap<>();
                    Map<Short, Column> columnIncludeMap = new TreeMap<>();
                    if (basic) {
                        columnBasicMap.put((short) ordinalPosition, column);
                    } else {
                        columnIncludeMap.put((short) ordinalPosition, column);
                    }
                    indexes.put(id, new Index(
                            id,
                            indexName,
                            columnBasicMap,
                            columnIncludeMap,
                            isUniq,
                            filterCondition,
                            indexDef));
                }
            }
        }
        rs.close();
        ps.close();
        return indexes;
    }

    @Override
    public Map.Entry<Integer, List<TableOption>> getOptions(Connection connection) throws SQLException {
        int oid = 0;
        List<TableOption> options = new ArrayList<>();
        PreparedStatement ps = connection.prepareStatement(SQL_PG_TABLE_OPTIONS);
        ps.setString(1, getFinalSchemaName(true));
        ps.setString(2, getFinalTableName(true));
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            oid = rs.getInt("oid");
            Array relOptionsArray = rs.getArray("reloptions");
            if (relOptionsArray != null) {
                String[] relOptions = (String[]) relOptionsArray.getArray();
                for (String option : relOptions) {
                    options.add(new TableOption(option));
                }
            }
/*
            log.info("Table Options for {}.{}: OID: {}, RelOptions: {}",
                    getFinalSchemaName(true), getFinalTableName(true), oid, relOptionsArray);
*/
        }
        return new AbstractMap.SimpleEntry<>(oid, options);
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
            pkQuery.append(column.columnName());
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
    public void createIndexes(Connection connection) {
        List<Index> indexes = getIndexes();
        indexes.forEach(i -> i.create(this, connection));
    }

    @Override
    public void createUniqueConstraints(Connection connection) {
        List<UniqueConstraint> uniqueConstraints = getUniqueConstraints();
        uniqueConstraints.forEach(uc -> uc.create(this, connection));
    }

    @Override
    public void createForeignKeys(Connection connection) {
        List<ForeignKey> foreignKeys = getForeignKeys();
        foreignKeys.forEach(fk -> fk.create(this, connection));
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

    @Override
    public void create(Connection connection) throws SQLException {
        if (!exists(connection)) {
            log.info("Creating table {}.{}", getSchemaName(), getTableName());
            String columnDefinition = getColumnDefinition();
            String query = DDL_CREATE_TABLE
                    .replace("$schemaName", getFinalSchemaName(true))
                    .replace("$tableName", getFinalTableName(true))
                    .replace("$columnDefinition", columnDefinition);
            if (this.getOptions() != null && !getOptions().isEmpty()) {
                query += " WITH (" + getOptionDefinition() + ")";
            }
            query = query.replace("\"\"","\"");
            log.info("{}", query);
            Statement statement = connection.createStatement();
            statement.execute(query);
            connection.commit();
        }
    }

    @Override
    public String buildOrderBy(Config config) {
        List<Column> columns = getPkColumns();
        if (columns == null || columns.isEmpty()) return "";
        if (config.columnToColumn() == null && config.expressionToColumn() == null) {
            String alias = config.fromTableAlias();
            String prefix = (alias != null && !alias.isEmpty()) ? alias + "." : "";
            String orderByBody = columns.stream()
                    .map(col -> prefix + col.columnName())
                    .collect(Collectors.joining(", "));
            return " ORDER BY " + orderByBody;
        } else {
            return "";
        }
    }

    @Override
    public boolean enrichTable(Connection session) throws SQLException {
        // if table exists, enrich it
        if (exists(session)) {
            setColumns(getAllColumns(session));
            setPkColumns(getPrimaryKeyColumns(session));
            Map.Entry<Integer, List<TableOption>> options = getOptions(session);
            setId(options.getKey());
            setOptions(options.getValue());
            return true;
        }
        return false;
    }

    private String getColumnDefinition() {
        StringBuilder columnDefinition = new StringBuilder();
        for (Column column : this.getColumns()) {
            columnDefinition.append(
                    column.isCaseSensitiveWord(column.columnName()) ?
                            "\"" + column.columnName() + "\"" :
                            column.columnName().toLowerCase()
                    ).append(" ")
                    .append(column.columnType());
            if (    column.columnType().equals("varchar") ||
                    column.columnType().equals("character varying") ||
                    column.columnType().equals("numeric") ||
                    column.columnType().equals("decimal") ||
                    column.columnType().equals("char") ||
                    column.columnType().equals("character") ||
                    column.columnType().equals("bpchar")
            ) {
                if (column.charOctetLength() > 0 && column.charOctetLength() < 100000) {
                    columnDefinition.append("(").append(column.charOctetLength());
                    if (column.decimalDigits() > 0) {
                        columnDefinition.append(", ").append(column.decimalDigits());
                    }
                    columnDefinition.append(")");
                }
            }
            if (column.isNullable() == 0) {
                columnDefinition.append(" NOT NULL");
            }
            if (column.defaultValue() != null) {
                columnDefinition.append(" DEFAULT ").append(column.defaultValue());
            }
            columnDefinition.append(", ");
        }
        // Remove the last comma and space
        if (!columnDefinition.isEmpty()) {
            columnDefinition.setLength(columnDefinition.length() - 2);
        }
        return columnDefinition.toString();
    }

    private String getOptionDefinition() {
        StringBuilder optionDefinition = new StringBuilder();
        if (this.getOptions() != null) {
            for (TableOption option : this.getOptions()) {
                if (!optionDefinition.isEmpty()) {
                    optionDefinition.append(", ");
                }
                optionDefinition.append(option.getOption());
            }
        }
        return optionDefinition.toString();
    }
}
