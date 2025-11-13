package org.bublik.cassandra.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import org.bublik.cassandra.storage.cassandraaddons.BatchEntity;
import org.bublik.cassandra.storage.cassandraaddons.CSObject;
import org.bublik.cassandra.storage.cassandraaddons.CSPartitionKey;
import org.bublik.core.model.*;
import org.bublik.core.service.Target;
import org.bublik.core.storage.Storage;
import org.bublik.core.storage.StorageClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

import static org.bublik.cassandra.storage.cassandraaddons.MM3.*;

public class CassandraStorageSource<K extends UUID, T extends Long, S extends CqlSession, R extends com.datastax.oss.driver.api.core.cql.ResultSet>
        extends CSStorage<K, T, S, R> implements Target {

    private static final Logger log = LoggerFactory.getLogger(CassandraStorageSource.class);

    private final int batchSize;
    private final CSPool csPool;
    protected final int threadCount;
    private final ConnectionProperty connectionProperty;

    public CassandraStorageSource(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        this.connectionProperty = connectionProperty;
        this.threadCount = connectionProperty.getThreadCount();
        this.batchSize = getBatchSize(connectionProperty);
        this.csPool = new CSPool(getStorageClass().getProperties(), connectionProperty.getThreadCount());
    }

    public CSPool getCsPool() {
        return csPool;
    }

    public int getBatchSize(ConnectionProperty connectionProperty) {
        String batchSize = connectionProperty.getToProperty().getProperty("batchSize");
        return batchSize == null ? 100 : Integer.parseInt(batchSize);
    }

    @Override
    public LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        return rangedBatchCS(chunk, tableName);
    }

    public LogMessage rangedBatchCS(Chunk<?, ?, ?, ?> chunk, String tableName) throws SQLException {
        int recordCount = 0;
        int batchCount = 0;
        long start = System.currentTimeMillis();
        CqlSession cqlSession = csPool.getCqlSession();
        CSObject csObject = CSObject.createCSObject(getCsPool(), chunk);
        com.datastax.oss.driver.api.core.cql.ResultSet resultSet = (com.datastax.oss.driver.api.core.cql.ResultSet) chunk.getResultSet();
        for (Row row : resultSet) {
            Map.Entry<TokenRange, Object[]> entry = getTokenRangedObjectsCS(
                    row,
                    csObject.getPartitionKeyMap(),
                    csObject.getCassandraColumnMap(),
                    csObject.getTokenRangeSet());
            Map<TokenRange, BatchEntity> tokenRangeBatchEntityMap = csObject.getMm3Batch().getTokenRangeMap();
            BatchEntity batchEntity = tokenRangeBatchEntityMap.get(entry.getKey());
            BatchStatementBuilder batchStatementBuilder = batchEntity.getBatchStatementBuilder();
            BatchableStatement<?> statement = csObject.getPreparedStatement().bind(entry.getValue());
//            log.info("{}", csObject.getQuery());
            batchStatementBuilder.addStatement(statement);
            batchEntity.increaseCounter();
            recordCount++;
            // batch_size_fail_threshold_in_kb: 50
            if (batchEntity.getCounter() == batchSize) {
                batchApply(batchStatementBuilder, cqlSession);
                batchEntity.resetCounter();
                batchCount++;
            }
        }
        for (Map.Entry<TokenRange, BatchEntity> entry : csObject.getMm3Batch().getTokenRangeMap().entrySet()) {
            if (entry.getValue().getCounter() > 0) {
                batchApply(entry.getValue().getBatchStatementBuilder(), cqlSession);
                batchCount++;
            }
        }
        long stop = System.currentTimeMillis();
        chunk.setRows(recordCount);
        return new LogMessage(start, stop, "BATCH APPLY (batches: " + batchCount + ")");
    }

    private void batchApply(BatchStatementBuilder batchStatementBuilder, CqlSession cqlSession) throws SQLException {
        try {
            BatchStatement batchStatement = batchStatementBuilder
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .setTimeout(Duration.ofSeconds(20))
                    .build();
            cqlSession.execute(batchStatement);
            batchStatementBuilder.clearStatements();
            batchStatement.clear();
        } catch (DriverException e) {
            throw new SQLException(e);
        }
    }

    private Map.Entry<TokenRange, Object[]> getTokenRangedObjectsCS(Row row,
                                                                    Map<Integer, CSPartitionKey> partitionKeyMap,
                                                                    Map<String, Column> stringCassandraColumnMap,
                                                                    Set<TokenRange> tokenRangeSet) throws SQLException {
        List<Object> objectList = new ArrayList<>();
        Map<Integer, byte[]> mapBytes = new TreeMap<>();
        for (Map.Entry<String, Column> entry : stringCassandraColumnMap.entrySet()) {
            String sourceColumn = entry.getKey().replaceAll("\"", "");
            String targetType = entry.getValue().columnType();
            switch (targetType) {
                case "smallint": {
                    short v = row.getShort(sourceColumn);
                    partitionKeyMap
                            .entrySet()
                            .stream()
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().columnName()))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.getKey(), smallIntToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "int": {
                    int v = row.getInt(sourceColumn);
                    partitionKeyMap
                            .entrySet()
                            .stream()
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().columnName()))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.getKey(), intToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "bigint": {
                    long v = row.getLong(sourceColumn);
                    partitionKeyMap
                            .entrySet()
                            .stream()
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().columnName()))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.getKey(), longToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "text": {
                    String v = row.getString(sourceColumn);
                    partitionKeyMap
                            .entrySet()
                            .stream()
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().columnName()))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.getKey(), stringToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "date": {
                    LocalDate date = row.getLocalDate(sourceColumn);
                    objectList.add(date);
                    break;
                }
                case "timestamp": {
                    Instant v = row.getInstant(sourceColumn);
                    partitionKeyMap
                            .entrySet()
                            .stream()
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().columnName()))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.getKey(), timestampToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "boolean": {
                    objectList.add(row.getBoolean(sourceColumn));
                    break;
                }
                case "blob": {
                    ByteBuffer buffer = row.getByteBuffer(sourceColumn);
                    objectList.add(buffer);
                    break;
                }
                case "float": {
                    objectList.add(row.getFloat(sourceColumn));
                    break;
                }
                case "decimal": {
                    objectList.add(row.getBigDecimal(sourceColumn));
                    break;
                }
                case "uuid": {
                    UUID uuid = row.getUuid(sourceColumn);
                    Map.Entry<Integer, CSPartitionKey> keyEntry = partitionKeyMap
                            .entrySet()
                            .stream()
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().columnName()))
                            .findFirst()
                            .orElseThrow();
                    assert uuid != null;
                    mapBytes.put(keyEntry.getKey(), uuidToBytes(uuid));
                    objectList.add(uuid);
                    break;
                }
                default:
                    break;
            }
        }
        byte[][] bytes = new byte[mapBytes.size()][];
        mapBytes.forEach((k, v) -> bytes[k] = v);
        TokenRange tokenRange = getTokenRange(tokenRangeSet, compositeToBytes(bytes));
        return new AbstractMap.SimpleEntry<>(tokenRange, objectList.toArray());
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public <C> C unwrap(Class<C> iface) {
        if (iface.isInstance(this)) {
            return (C) this;
        } else {
            throw new RuntimeException("No object found that implements the interface: " + iface.getName());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    @Override
    public void start(List<Config> configs, boolean sync, int rows, Storage<K, T, S, R> targetStorage, String tableName) throws SQLException {

    }

    @Override
    public void createOutbox(String tableName) throws SQLException {

    }

    @Override
    public void dropOutboxTable(boolean sync, String tableName) throws SQLException {

    }

    @Override
    public List<Config> copyConfigs(List<Config> cfgs) {
        return List.of();
    }

    @Override
    public List<Chunk<K, T, S, R>> getChunkList(List<Config> configs, String chunkTableName, Storage<K, T, S, R> targetStorage) throws SQLException {
        return List.of();
    }

    @Override
    public String buildStartEndOfChunk(Config config, String chunkTableName) {
        return "";
    }

    @Override
    public void closeStorage() {

    }

    @Override
    public String buildFetchStatement(Config config, Table2Table<S> t2t) {
        return "";
    }

    @Override
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?, ?, ?> chunk) {
        return Map.of();
    }

    @Override
    public Map<Table<S>, Table<S>> configsToTables(List<Config> configs, Storage<K, T, S, R> targetStorage) {
        return Map.of();
    }

    @Override
    public Table configToTable(String schemaName, String tableName) {
        return null;
    }

    @Override
    public Table getTagetTableBySourceTable(Table table) {
        return null;
    }

    @Override
    public Table getSourceTableByTargetTable(Table table) {
        return null;
    }
}
