package dev.bublik.cassandra.model;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import dev.bublik.core.model.*;
import dev.bublik.cassandra.constants.CSNativeType;
import dev.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static dev.bublik.cassandra.constants.SQLConstants.SQL_ALL_COLUMNS;
import static dev.bublik.core.util.Utils.getStackTrace;

public class CSTable extends Table {
    private static final Logger log = LoggerFactory.getLogger(CSTable.class);
    private List<Column> partitionKey;
    private List<Column> clusteringKey;
    private List<UDTColumn> udtColumns;

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

    public List<UDTColumn> getUdtColumns() {
        return udtColumns;
    }

    public void setUdtColumns(List<UDTColumn> udtColumns) {
        this.udtColumns = udtColumns;
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
    public <S extends AutoCloseable> List<Column> getAllColumns(S session) {
        List<Column> columns = new ArrayList<>();
        CqlSession cqlSession = (CqlSession) session;
        try {
            ResultSet resultSet = cqlSession.execute(
                    SQL_ALL_COLUMNS,
                    getSchemaName(),
                    getTableName()
            );
            List<UDTColumn> udtColumns = new ArrayList<>();
            for (Row row : resultSet) {
                String kind = row.getString("kind");
                assert kind != null;
                String columnName = row.getString("column_name");
                String columnType = row.getString("type");
                assert columnType != null;
                Column.UdtType udtType = getUdtTypeByTypeName(cqlSession, columnType);
                Pattern isFrozen = Pattern.compile("(?i)\\bfrozen\\s*<");
                Pattern isCollection = Pattern.compile("(?i)\\b(frozen\\s*<\\s*)?(list|set|map)\\s*<");
                Column column = new Column(
                        row.getInt("position"),
                        columnName,
                        columnType,
                        row.getString("clustering_order"),
                        kind.equals("static"),
                        kind.equals("partition_key"),
                        kind.equals("clustering"),
                        udtType,
                        isFrozen.matcher(columnType).find(),
                        isCollection.matcher(columnType).find()
                );
                columns.add(column);
                if (udtType != null) {
                    udtColumns.add(new UDTColumn(column, getUdtType(cqlSession, columnType)));
                }
            }
        } catch (Exception e) {
            log.error("Error while getting all columns for table {}: {}", getTableName(), getStackTrace(e));
        }
        setUdtColumns(udtColumns);
        return columns;
    }

    public Column.UdtType getUdtTypeByTypeName(CqlSession cqlSession, String columnType) {
        try {
            CSNativeType.valueOf(columnType.toUpperCase());
            return null;
        } catch (IllegalArgumentException e) {
            UserDefinedType udt = getUdtType(cqlSession, columnType);
            if (udt == null) {
                return null;
            }
            String[] typeFieldNames = udt.getFieldNames().stream().map(CqlIdentifier::toString).toArray(String[]::new);
            String[] typeFieldTypes = udt.getFieldTypes().stream().map(Object::toString).toArray(String[]::new);
            List<Column> typeColumns = new ArrayList<>();
            for (int i = 0; i < typeFieldNames.length; i++) {
                typeColumns.add( new Column(typeFieldNames[i], typeFieldTypes[i].toLowerCase()));
            }
            return new Column.UdtType(columnType, typeColumns);
        }
    }

    public UserDefinedType getUdtType(CqlSession cqlSession, String columnType) {
        String newColumnTypeName;
        Pattern pattern = Pattern.compile("(frozen)<(.*)>");
        Matcher matcher = pattern.matcher(columnType);
        if (columnType.contains("frozen") && matcher.find()) {
            newColumnTypeName = columnType.substring(7, columnType.length() - 1);
        } else {
            newColumnTypeName = columnType;
        }
        if (newColumnTypeName.matches("(.*)<(.*)>$")) {
            return null;
        }
        return cqlSession
                .getMetadata()
                .getKeyspace(getSchemaName())
                .get()
                .getUserDefinedType(newColumnTypeName)
                .get();
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
    public <S extends AutoCloseable> boolean enrichTable(S session) {
        List<Column> allColumns = getAllColumns(session);
        setColumns(allColumns);
        setClusteringKey(allColumns.stream().filter(Column::isClusteringKey).toList());
        setPartitionKey(allColumns.stream().filter(Column::isPartitionKey).toList());
        return true;
    }

    @Override
    public String buildOrderBy(Config config) {
        return "";
    }

    public record UDTColumn(Column column, UserDefinedType udt){}

    private CSTable(Builder builder) {
        super(builder);
        this.partitionKey = builder.partitionKey;
        this.clusteringKey = builder.clusteringKey;
        this.udtColumns = builder.udtColumns;
    }

    public static class Builder extends Table.Builder<CSTable, Builder> {
        protected List<Column> partitionKey = new ArrayList<>();
        protected List<Column> clusteringKey = new ArrayList<>();
        protected List<UDTColumn> udtColumns = new ArrayList<>();

        protected Builder(String keySpace, String tableName) {
            super(keySpace, tableName);
        }

        @Override
        protected Builder self() {
            return this;
        }

        public Builder partitionKey(List<Column> partitionKey) { this.partitionKey = partitionKey; return this; }
        public Builder clusteringKey(List<Column> clusteringKey) { this.clusteringKey = clusteringKey; return this; }
        public Builder udtColumns(List<UDTColumn> udtColumns) { this.udtColumns = udtColumns; return this; }

        public Builder addPartitionKeyColumn(Column column) { this.partitionKey.add(column); return this; }
        @Override
        public CSTable build() {
            validate();
            return new CSTable(this);
        }
    }
}
