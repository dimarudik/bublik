package org.bublik.cassandra.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import org.bublik.cassandra.model.CSTable;
import org.bublik.cassandra.service.CSTableService;
import org.bublik.cassandra.storage.cassandraaddons.BatchEntity;
import org.bublik.cassandra.storage.cassandraaddons.CSObject;
import org.bublik.cassandra.storage.cassandraaddons.CSPartitionKey;
import org.bublik.core.model.*;
import org.bublik.core.storage.JDBCStorage;
import org.bublik.core.storage.StorageClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

import static org.bublik.cassandra.storage.cassandraaddons.MM3.*;

public class CassandraStorage<K extends UUID, T extends Long, S extends CqlSession, R extends com.datastax.oss.driver.api.core.cql.ResultSet>
        extends CSStorage<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(CassandraStorage.class);
    private final int batchSize;

    public CassandraStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        this.batchSize = getBatchSize(connectionProperty);
    }

    @Override
    public LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        if (chunk.getSourceStorage() instanceof CSStorage) {
            com.datastax.oss.driver.api.core.cql.ResultSet resultSet = chunk.getResultSet();
            return rangedBatch(chunk, resultSet);
//            return null;
        } else if (chunk.getSourceStorage() instanceof JDBCStorage) {
/*
        if (isChunkProcessed(cqlSession, (Integer) chunk.getId(), chunk.getConfig().fromTaskName(), tableName)) {
            return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(),
                    "Chunk id = " + chunk.getId() + " already processed, skip it");
        }
*/
            ResultSet resultSet = (ResultSet) chunk.getResultSet();
            LogMessage logMessage = rangedBatch(chunk, resultSet);
//        insertProcessedChunkInfo(cqlSession, (int) chunk.getId(), recordCount, chunk.getConfig().fromTaskName(), tableName);
            return logMessage;
        }
        return null;
    }

    public LogMessage rangedBatch(Chunk<K, T, S, R> chunk, ResultSet resultSet) throws SQLException {
        int recordCount = 0;
        int batchCount = 0;
        long start = System.currentTimeMillis();
        CqlSession cqlSession = chunk.getTargetSession();
        CSObject csObject = CSObject.createCSObject(getCsPool(), chunk);
        Map<TokenRange, BatchEntity> tokenRangeBatchEntityMap = csObject.getMm3Batch().getTokenRangeMap();
        while (resultSet.next()) {
            Map.Entry<TokenRange, Object[]> entry = getTokenRangedObjects(
                    resultSet,
                    csObject.getPartitionKeyMap(),
                    csObject.getCassandraColumnMap(),
                    csObject.getTokenRangeSet());
            BatchEntity batchEntity = tokenRangeBatchEntityMap.get(entry.getKey());
            BatchStatementBuilder batchStatementBuilder = batchEntity.getBatchStatementBuilder();
            BatchableStatement<?> statement = csObject.getPreparedStatement().bind(entry.getValue());
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

    public LogMessage rangedBatch(Chunk<K, T, S, R> chunk, com.datastax.oss.driver.api.core.cql.ResultSet resultSet) throws SQLException {
        int recordCount = 0;
        int batchCount = 0;
        long start = System.currentTimeMillis();
        CqlSession cqlSession = chunk.getTargetSession();
        CSObject csObject = CSObject.createCSObject(getCsPool(), chunk);
        Map<TokenRange, BatchEntity> tokenRangeBatchEntityMap = csObject.getMm3Batch().getTokenRangeMap();
        for (Row row : resultSet) {
            Map.Entry<TokenRange, Object[]> entry = getTokenRangedObjects(
                    row,
                    chunk.getT2t(),
                    csObject.getTokenRangeSet());
            Arrays.stream(entry.getValue()).forEach(System.out::println);
            System.out.println(csObject.getPreparedStatement().getQuery());
            BatchEntity batchEntity = tokenRangeBatchEntityMap.get(entry.getKey());
            BatchStatementBuilder batchStatementBuilder = batchEntity.getBatchStatementBuilder();
            BatchableStatement<?> statement = csObject.getPreparedStatement().bind(entry.getValue());
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

    private Map.Entry<TokenRange, Object[]> getTokenRangedObjects(ResultSet resultSet,
                                                                  Map<Integer, CSPartitionKey> partitionKeyMap,
                                                                  Map<String, Column> stringCassandraColumnMap,
                                                                  Set<TokenRange> tokenRangeSet) throws SQLException {
        List<Object> objectList = new ArrayList<>();
        Map<Integer, byte[]> mapBytes = new TreeMap<>();
        for (Map.Entry<String, Column> entry : stringCassandraColumnMap.entrySet()) {
            String sourceColumn = entry.getKey().replaceAll("\"", "");
            String targetType = entry.getValue().columnType();
            switch (targetType) {
                case "tinyint" : {
                    byte v = (byte) resultSet.getInt(sourceColumn);
                    partitionKeyMap
                            .entrySet()
                            .stream()
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().columnName()))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.getKey(), byteToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "smallint" : {
                    short v = resultSet.getShort(sourceColumn);
                    partitionKeyMap
                            .entrySet()
                            .stream()
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().columnName()))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.getKey(), smallIntToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "int" : {
                    int v = resultSet.getInt(sourceColumn);
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
                    long v = resultSet.getLong(sourceColumn);
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
                    String v = resultSet.getString(sourceColumn);
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
                    Timestamp timestamp = resultSet.getTimestamp(sourceColumn);
                    long l = timestamp.getTime();
                    LocalDate date = Instant.ofEpochMilli(l)
                            .atZone(ZoneId.systemDefault()).toLocalDate();
                    objectList.add(date);
                    break;
                }
                case "timestamp": {
                    Instant v = resultSet.getTimestamp(sourceColumn).toInstant();
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
                    objectList.add(resultSet.getBoolean(sourceColumn));
                    break;
                }
                case "blob": {
                    byte[] bytes = resultSet.getBytes(sourceColumn);
                    if (bytes != null) {
                        ByteBuffer buffer = ByteBuffer.wrap(bytes);
                        objectList.add(buffer);
                    } else {
                        objectList.add(null);
                    }
                    break;
                }
                case "float": {
                    objectList.add(resultSet.getFloat(sourceColumn));
                    break;
                }
                case "decimal": {
                    objectList.add(resultSet.getBigDecimal(sourceColumn));
                    break;
                }
                case "uuid": {
                    Object v = resultSet.getObject(sourceColumn);
                    UUID uuid;
                    try {
                        uuid = (UUID) v;
                    } catch (ClassCastException e) {
                        uuid = UUID.fromString((String) v);
                    }
/*
                    int position = partitionKeyList
                            .stream()
                            .filter(e -> e.columnName().equals(entry.getValue().columnName()))
                            .findFirst()
                            .map(Column::columnPosition)
                            .orElseThrow();
                    mapBytes.put(position, uuidToBytes(uuid));
*/
                    Map.Entry<Integer, CSPartitionKey> keyEntry = partitionKeyMap
                            .entrySet()
                            .stream()
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().columnName()))
                            .findFirst()
                            .orElseThrow();
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

    private Map.Entry<TokenRange, Object[]> getTokenRangedObjects(Row row,
                                                                  Table2Table<S> t2t,
                                                                  Set<TokenRange> tokenRangeSet) throws SQLException {
        List<Object> objectList = new ArrayList<>();
        CSTable<?> targetTable = (CSTable<?>) t2t.targetTable();
        List<Column> targetPartKeys = targetTable.getPartitionKey();
        Map<Column, Column> column2Column = new HashMap<>();
        t2t.column2Columns().forEach((c) -> column2Column.put(c.sourceColumn(), c.targetColumn()));
        Map<Integer, byte[]> mapBytes = new TreeMap<>();
//        for (Map.Entry<String, Column> entry : stringCassandraColumnMap.entrySet()) {
        for (Map.Entry<Column, Column> entry: column2Column.entrySet()) {
//            String sourceColumn = entry.getKey().replaceAll("\"", "");
//            String targetType = entry.getValue().columnType();
            String targetType = entry.getValue().columnType();
            String sClmName = entry.getKey().columnName();
            String tClmName = entry.getValue().columnName();
            switch (targetType) {
                case "tinyint" : {
                    byte v = row.getByte(sClmName);
                    targetPartKeys
                            .stream()
                            .filter(e -> e.columnName().equals(tClmName))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.columnPosition(), byteToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "smallint" : {
                    short v = row.getShort(sClmName);
                    targetPartKeys
                            .stream()
                            .filter(e -> e.columnName().equals(tClmName))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.columnPosition(), smallIntToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "int" : {
                    int v = row.getInt(sClmName);
                    targetPartKeys
                            .stream()
                            .filter(e -> e.columnName().equals(tClmName))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.columnPosition(), intToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "bigint": {
                    long v = row.getLong(sClmName);
                    targetPartKeys
                            .stream()
                            .filter(e -> e.columnName().equals(tClmName))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.columnPosition(), longToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "text": {
                    String v = row.getString(sClmName);
//                    Integer ttl = row.get("ttl(" + sourceColumn + ")", Integer.class);
                    targetPartKeys
                            .stream()
                            .filter(e -> e.columnName().equals(tClmName))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.columnPosition(), stringToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "date": {
                    LocalDate date = row.getLocalDate(sClmName);
                    objectList.add(date);
                    break;
                }
                case "timestamp": {
                    Instant v = row.getInstant(sClmName);
                    targetPartKeys
                            .stream()
                            .filter(e -> e.columnName().equals(tClmName))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.columnPosition(), timestampToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "boolean": {
                    objectList.add(row.getBoolean(sClmName));
                    break;
                }
                case "blob": {
                    ByteBuffer buffer = row.getByteBuffer(sClmName);
                    objectList.add(buffer);
                    break;
                }
                case "float": {
                    objectList.add(row.getFloat(sClmName));
                    break;
                }
                case "decimal": {
                    objectList.add(row.getBigDecimal(sClmName));
                    break;
                }
                case "uuid": {
                    UUID uuid = row.getUuid(sClmName);
                    int position = targetPartKeys
                            .stream()
                            .filter(e -> e.columnName().equals(tClmName))
                            .findFirst()
                            .map(Column::columnPosition)
                            .orElseThrow();
                    mapBytes.put(position, uuidToBytes(uuid));
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
}
