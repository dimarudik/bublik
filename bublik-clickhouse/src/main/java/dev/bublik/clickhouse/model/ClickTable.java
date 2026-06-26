package dev.bublik.clickhouse.model;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import dev.bublik.core.model.*;
import dev.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static dev.bublik.clickhouse.constants.SQLConstants.SQL_ALL_COLUMNS;

public class ClickTable extends Table {
    private static final Logger log = LoggerFactory.getLogger(ClickTable.class);

    public ClickTable(String schemaName, String tableName) {
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
    public <S extends AutoCloseable> List<Column> getAllColumns(S session) throws SQLException {
        List<Column> columns = new ArrayList<>();
        Client connection = (Client) session;
        List<GenericRecord> records = connection.queryAll(SQL_ALL_COLUMNS,
                java.util.Map.of("db", getSchemaName(), "table", getTableName()));

        for (GenericRecord record : records) {
            long position = record.getLong("position");

            Column column = new Column(
                    (int) position,
                    record.getString("name"),
                    record.getString("type"),
                    record.getString("default_expression"));
            columns.add(column);
        }
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
    public <S extends AutoCloseable> boolean enrichTable(S session) throws SQLException {
        List<Column> allColumns = getAllColumns(session);
        setColumns(allColumns);
        return true;
    }

    @Override
    public String buildOrderBy(Config config) {
        return "";
    }
}
