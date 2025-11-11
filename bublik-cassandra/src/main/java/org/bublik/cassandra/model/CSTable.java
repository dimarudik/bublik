package org.bublik.cassandra.model;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import org.bublik.cassandra.service.CSTableService;
import org.bublik.core.model.*;
import org.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CSTable<S extends CqlSession> extends Table<S> {
    private static final Logger log = LoggerFactory.getLogger(CSTable.class);
    private List<Column> partitionKey;
    private List<Column> clusteringKey;

    public CSTable(String schemaName, String tableName, List<Column> partitionKey, List<Column> clusteringKey) {
        super(schemaName, tableName);
        this.partitionKey = partitionKey;
        this.clusteringKey = clusteringKey;
    }

    public void setPartitionKey(List<Column> partitionKey) {
        this.partitionKey = partitionKey;
    }

    public void setClusteringKey(List<Column> clusteringKey) {
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
    public List<Column> getAllColumns(CqlSession cqlSession) {
        List<Column> columns = new ArrayList<>();
        Metadata metadata = cqlSession.getMetadata();
        KeyspaceMetadata keyspaceMetadata = metadata
                .getKeyspace(getSchemaName())
                .orElseThrow();
        Map<CqlIdentifier, ColumnMetadata> mapColumnMetaData = keyspaceMetadata
                .getTable(getTableName())
                .orElseThrow()
                .getColumns();
        List<ColumnMetadata> columnMetadata = mapColumnMetaData.values().stream().toList();
        columnMetadata.forEach(c -> {
            String columnName = c.getName().toString();
            String columnType = c.getType().toString().toLowerCase();
            columns.add(new Column(0, columnName, columnType, null, null, null, null, null, 0, null, 0, null));
        });
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
    public boolean enrichTable(S session) {
        List<Column> partitionKey = CSTableService.getKey(session, this, "partition_key");
        List<Column> clusteringKey = CSTableService.getKey(session, this, "clustering");
        setClusteringKey(clusteringKey);
        setPartitionKey(partitionKey);
        setColumns(getAllColumns(session));
        return true;
    }
}
