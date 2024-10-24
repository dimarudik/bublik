package org.bublik.storage.cassandraaddons;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import org.bublik.constants.PGKeywords;
import org.bublik.model.*;

import java.util.*;

public class CSObject {
    private final CqlSession cqlSession;
    private Metadata metadata;
    private Set<TokenRange> tokenRangeSet;
    private MM3Batch mm3Batch;
    private Map<String, CassandraColumn> cassandraColumnMap;
    private String query;
    private PreparedStatement preparedStatement;
    private Map<Integer, CSPartitionKey> partitionKeyMap;

    public CSObject(CqlSession cqlSession) {
        this.cqlSession = cqlSession;
    }

    public CqlSession getCqlSession() {
        return cqlSession;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public Set<TokenRange> getTokenRangeSet() {
        return tokenRangeSet;
    }

    public void setTokenRangeSet(Set<TokenRange> tokenRangeSet) {
        this.tokenRangeSet = tokenRangeSet;
    }

    public Map<String, CassandraColumn> getCassandraColumnMap() {
        return cassandraColumnMap;
    }

    public void setCassandraColumnMap(Map<String, CassandraColumn> cassandraColumnMap) {
        this.cassandraColumnMap = cassandraColumnMap;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public void setPreparedStatement(PreparedStatement preparedStatement) {
        this.preparedStatement = preparedStatement;
    }

    public Map<Integer, CSPartitionKey> getPartitionKeyMap() {
        return partitionKeyMap;
    }

    public void setPartitionKeyMap(Map<Integer, CSPartitionKey> partitionKeyMap) {
        this.partitionKeyMap = partitionKeyMap;
    }

    public CSObject metadata() {
        Metadata m = getCqlSession().getMetadata();
        setMetadata(m);
        return this;
    }

    public MM3Batch getMm3Batch() {
        return mm3Batch;
    }

    public void setMm3Batch(MM3Batch mm3Batch) {
        this.mm3Batch = mm3Batch;
    }

    public CSObject tokenRangeSet() {
        Set<TokenRange> t = getMetadata().getTokenMap().orElseThrow().getTokenRanges();
        setTokenRangeSet(t);
        return this;
    }

    public CSObject mm3batch() {
        MM3Batch mm3 = MM3Batch.createMM3Batch();
        mm3.initMM3Batch(getTokenRangeSet());
        setMm3Batch(mm3);
        return this;
    }

    public CSObject cassandraColumnMap(Chunk<?> chunk) {
        Map<String, CassandraColumn> csmap = readTargetColumnsAndTypes(chunk, metadata);
        setCassandraColumnMap(csmap);
        return this;
    }

    public CSObject query(Chunk<?> chunk) {
        String q = buildInsertStatement(chunk, getCassandraColumnMap());
        setQuery(q);
        return this;
    }

    public CSObject preparedStatement() {
        PreparedStatement p = getCqlSession().prepare(getQuery());
        setPreparedStatement(p);
        return this;
    }

    public CSObject partitionKeyMap(Chunk<?> chunk) {
        Map<Integer, CSPartitionKey> map = readPartitonKeyMap(chunk);
        setPartitionKeyMap(map);
        return this;
    }

    private Map<Integer, CSPartitionKey> readPartitonKeyMap(Chunk<?> chunk) {
        Config config = chunk.getConfig();
        ResultSet resultSet = cqlSession.execute(
                "select column_name, type, position from system_schema.columns " +
                        "where keyspace_name = ? and table_name = ? and kind = 'partition_key' allow filtering",
                config.toSchemaName(),
                config.toTableName()
        );
        Map<Integer, CSPartitionKey> map = new TreeMap<>();
        for (Row row : resultSet) {
            map.put(row.getInt("position"),
                    new CSPartitionKey(row.getString("type"), row.getString("column_name")));
        }
        return map;
    }

    private Map<String, CassandraColumn> readTargetColumnsAndTypes(Chunk<?> chunk, Metadata metadata) {
        Map<String, CassandraColumn> columnMap = new HashMap<>();
        Config config = chunk.getConfig();
        KeyspaceMetadata keyspaceMetadata = metadata
                .getKeyspace(config.toSchemaName())
                .orElseThrow();
        Map<CqlIdentifier, ColumnMetadata> mapColumnMetaData = keyspaceMetadata
                .getTable(config.toTableName())
                .orElseThrow()
//                .get()
                .getColumns();
        List<ColumnMetadata> columnMetadata = mapColumnMetaData.values().stream().toList();

        Map<String, String> columnToColumnMap = chunk.getConfig().columnToColumn();
        Map<String, String> expressionToColumnMap = chunk.getConfig().expressionToColumn();

        for (ColumnMetadata c : columnMetadata) {
            if (chunk.getConfig().columnToColumn() != null) {
                columnToColumnMap.entrySet()
                        .stream()
                        .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(c.getName().toString()))
                        .forEach(v -> columnMap.put(v.getKey(), new CassandraColumn(0, v.getValue(), c.getType().toString().toLowerCase())));
            }
            if (chunk.getConfig().expressionToColumn() != null) {
                expressionToColumnMap.entrySet()
                        .stream()
                        .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(c.getName().toString()))
                        .forEach(v -> columnMap.put(c.getName().toString(), new CassandraColumn(0, c.getName().toString(), c.getType().toString().toLowerCase())));
            }
        }
        return columnMap;
    }

    private String buildInsertStatement(Chunk<?> chunk, Map<String, CassandraColumn> stringCassandraColumnMap) {
        List<String> targetColumns = stringCassandraColumnMap.values().stream().map(Column::getColumnName).toList();
        return PGKeywords.INSERT + " " + PGKeywords.INTO + " " +
                chunk.getConfig().toSchemaName() + "." +
                chunk.getConfig().toTableName() + " (" +
                String.join(", ", targetColumns) + ") " +
                PGKeywords.VALUES + " (" + " :" +
                String.join(", :", targetColumns) +
//                targetColumns.stream().map(c -> "?").collect(Collectors.joining(",")) +
                ");";
    }

    public static CSObject createCSObject(CqlSession cqlSession, Chunk<?> chunk) {
        return new CSObject(cqlSession)
                .metadata()
                .tokenRangeSet()
                .mm3batch()
                .cassandraColumnMap(chunk)
                .query(chunk)
                .preparedStatement()
                .partitionKeyMap(chunk);
    }
}
