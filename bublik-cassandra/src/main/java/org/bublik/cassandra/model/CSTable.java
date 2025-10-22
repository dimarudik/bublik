package org.bublik.cassandra.model;

import org.bublik.core.model.*;
import org.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class CSTable extends Table {
    private static final Logger log = LoggerFactory.getLogger(CSTable.class);
    private final List<Column> partitionKey;
    private final List<Column> clusteringKey;

    public CSTable(String schemaName, String tableName, List<Column> partitionKey, List<Column> clusteringKey) {
        super(schemaName, tableName);
        this.partitionKey = partitionKey;
        this.clusteringKey = clusteringKey;
    }

    public List<Column> getPartitionKey() {
        return partitionKey;
    }

    public List<Column> getClusteringKey() {
        return clusteringKey;
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
    public List<Column> getAllColumns(Connection connection) throws SQLException {
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
}
