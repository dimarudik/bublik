package dev.bublik.core.model;

import dev.bublik.core.storage.Storage;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class PseudoTable<S extends AutoCloseable> extends Table<S> {

    public PseudoTable(String schemaName, String tableName) {
        super(schemaName, tableName);
    }

    @Override
    public boolean exists(Connection connection) throws SQLException {
        return false;
    }

    @Override
    public String getFinalTableName(boolean withQuotes) {
        return "";
    }

    @Override
    public String getFinalSchemaName() {
        return "";
    }

    @Override
    public String getHintClause() {
        return "";
    }

    @Override
    public List<Column> getAllColumns(S connection) throws SQLException {
        return List.of();
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
        return false;
    }

    @Override
    public String buildOrderBy(List<Column> pkColumns) {
        return "";
    }
}
