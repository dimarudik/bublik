package dev.bublik.mssql.model;

import dev.bublik.core.model.*;
import dev.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    public List<Column> obtainClusteringKey(S connection) throws SQLException {
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
}
