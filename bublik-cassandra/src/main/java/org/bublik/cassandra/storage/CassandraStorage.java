package org.bublik.cassandra.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import org.bublik.cassandra.model.CSTable;
import org.bublik.cassandra.storage.cassandraaddons.*;
import org.bublik.core.model.*;
import org.bublik.core.storage.JDBCStorage;
import org.bublik.core.storage.Storage;
import org.bublik.core.storage.StorageClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

import static org.bublik.cassandra.constants.SQLConstants.DDL_CREATE_LOCAL_OUTBOX_TABLE;
import static org.bublik.cassandra.storage.cassandraaddons.MM3.*;
import static org.bublik.core.util.Utils.getStackTrace;

public class CassandraStorage<K extends UUID, T extends Long, S extends CqlSession, R extends com.datastax.oss.driver.api.core.cql.ResultSet>
        extends CSStorage<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(CassandraStorage.class);
    private final int batchSize;

    public CassandraStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        this.batchSize = getBatchSize(connectionProperty);
    }

    @Override
    public void createLocalOutbox(String tableName) throws SQLException {
        CqlSession cqlSession = getSession();
        cqlSession.execute(DDL_CREATE_LOCAL_OUTBOX_TABLE.replace("$tableName", getOutboxTableName(tableName)));
        log.info("Local outbox table created successfully");
    }

    @Override
    public LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        Storage<K, T, S, R> sourceStorage = chunk.getSourceStorage();
        if (sourceStorage instanceof CSStorage) {
            com.datastax.oss.driver.api.core.cql.ResultSet resultSet = chunk.getResultSet();
//            insertProcessedChunkInfo(chunk, tableName);
            return rangedByTokenRangeAndTtlAntTimestampBatch(chunk, resultSet);
        } else if (sourceStorage instanceof JDBCStorage) {
            ResultSet resultSet = (ResultSet) chunk.getResultSet();
//            insertProcessedChunkInfo(chunk, tableName);
            return rangedByTokenRangeBatch(chunk, resultSet);
        }
        throw new RuntimeException("Unknown storage type");
    }


    public LogMessage rangedByTokenRangeBatch(Chunk<K, T, S, R> chunk, ResultSet resultSet) throws SQLException {
        int recordCount = 0;
        int batchCount = 0;
        long start = System.currentTimeMillis();
        CqlSession cqlSession = chunk.getTargetSession();
        CSObject csObject = CSObject.createCSObject(getCsPool(), chunk);
//        log.info("{}", csObject.getQuery());
        Table2Table<?> t2t = chunk.getT2t();
        Map<TokenRange, BatchEntity> tokenRangeBatchEntityMap = csObject.getMm3Batch().getTokenRangeMap();
        while (resultSet.next()) {
            Map.Entry<TokenRange, Object[]> entry = getTokenRangedObjects(
                    resultSet,
                    csObject.getPartitionKeyMap(),
                    csObject.getCassandraColumnMap(),
                    csObject.getTokenRangeSet(),
                    t2t);
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
        chunk.setCopied(recordCount);
        return new LogMessage(start, stop, "(batches: " + batchCount + ")");
    }

    private void batchApply(BatchStatementBuilder batchStatementBuilder, CqlSession cqlSession) {
        try {
            BatchStatement batchStatement = batchStatementBuilder
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .setTimeout(Duration.ofSeconds(20))
                    .build();
            cqlSession.execute(batchStatement);
            batchStatementBuilder.clearStatements();
            batchStatement.clear();
        } catch (Exception e) {
            log.error("{}", getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    public LogMessage rangedByTokenRangeAndTtlAntTimestampBatch(Chunk<K, T, S, R> chunk,
                                                                com.datastax.oss.driver.api.core.cql.ResultSet resultSet) throws SQLException {
        int recordCount = 0;
        int batchRecordCount = 0;
        int batchCount = 0;
        long start = System.currentTimeMillis();
        CqlSession cqlSession = chunk.getTargetSession();
        Set<TokenRange> tokenRangeSet = getCsPool().tokenRanges();
        MM3Batch mm3Batch = MM3Batch.createMM3Batch();
        mm3Batch.initMM3Batch(tokenRangeSet);
        Map<TokenRange, BatchEntity> tokenRangeBatchEntityMap = mm3Batch.getTokenRangeMap();

        for (Row row : resultSet) {
            CSRecord csRecord = getCSRecord(row, chunk.getT2t(), tokenRangeSet);

            BatchEntity batchEntity = tokenRangeBatchEntityMap.get(csRecord.tokenRange());
            BatchStatementBuilder batchStatementBuilder = batchEntity.getBatchStatementBuilder();

//            System.out.println("------------------------------------------------------------");
            Map<CSValueAttribute, List<CSValue>> map = csRecord.values()
                    .stream()
                    .filter(CSValue::isRegular)
                    .collect(Collectors.groupingBy(CSValue::groupByAttribute));

            for (Map.Entry<CSValueAttribute, List<CSValue>> entry : map.entrySet()) {
                List<CSValue> csValues = new ArrayList<>(entry.getValue());
                csValues.addAll(csRecord.values()
                        .stream()
                        .filter(CSValue::isNonRegular)
                        .toList()
                );
                Integer ttl = entry.getKey().ttl();
                Long timestamp = entry.getKey().timestamp();
                CSRecord record = new CSRecord(csRecord.tokenRange(), csValues, new CSValueAttribute(ttl, timestamp));
                String insertQuery = record.buildInsertStatement(chunk);
                List<Object> objects = new ArrayList<>(record.values().stream().map(CSValue::value).toList());
                if (ttl != null) {
                    objects.add(ttl);
                }
                if (timestamp != null) {
                    objects.add(timestamp);
                }
//                System.out.println(insertQuery);
//                System.out.println(objects);

                PreparedStatement ps = cqlSession.prepare(insertQuery);
                BatchableStatement<?> statement = ps.bind(objects.toArray());

                batchStatementBuilder.addStatement(statement);
                batchEntity.increaseCounter();
//                System.out.println("recordCount = " + batchRecordCount);
                batchRecordCount++;

                // batch_size_fail_threshold_in_kb: 50
                if (batchEntity.getCounter() == batchSize) {
                    batchApply(batchStatementBuilder, cqlSession);
                    batchEntity.resetCounter();
                    batchCount++;
                }

            }
            recordCount++;
        }

        // тут должно быть другое условие
        for (Map.Entry<TokenRange, BatchEntity> entry : mm3Batch.getTokenRangeMap().entrySet()) {
            if (entry.getValue().getCounter() > 0) {
                batchApply(entry.getValue().getBatchStatementBuilder(), cqlSession);
                batchCount++;
            }
        }

        long stop = System.currentTimeMillis();
        chunk.setCopied(recordCount);
        return new LogMessage(start, stop, "(batches: " + batchCount + ")");
    }

    private Map.Entry<TokenRange, Object[]> getTokenRangedObjects(ResultSet resultSet,
                                                                  Map<Integer, CSPartitionKey> partitionKeyMap,
                                                                  Map<String, Column> stringCassandraColumnMap,
                                                                  Set<TokenRange> tokenRangeSet,
                                                                  Table2Table<?> t2t) throws SQLException {
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
        if (t2t.ttlColumn() != null) {
            int ttl = resultSet.getInt(t2t.ttlColumn().columnName());
            objectList.add(ttl);
        }
        if (t2t.timestampColumn() != null) {
            long timestamp = resultSet.getLong(t2t.timestampColumn().columnName());
            objectList.add(timestamp);
        }
        byte[][] bytes = new byte[mapBytes.size()][];
        mapBytes.forEach((k, v) -> bytes[k] = v);
        TokenRange tokenRange = getTokenRange(tokenRangeSet, compositeToBytes(bytes));
        return new AbstractMap.SimpleEntry<>(tokenRange, objectList.toArray());
    }

    private CSRecord getCSRecord(Row row, Table2Table<S> t2t, Set<TokenRange> tokenRangeSet) throws SQLException {
        List<CSValue> objectList = new ArrayList<>();
        CSTable<?> targetTable = (CSTable<?>) t2t.targetTable();
        List<Column> targetPartKeys = targetTable.getPartitionKey();
        Map<Column, Column> column2Column = new HashMap<>();
        t2t.column2Columns().forEach((c) -> column2Column.put(c.sourceColumn(), c.targetColumn()));
        Map<Integer, byte[]> mapBytes = new TreeMap<>();
        Integer recordTtl = null;
        Long recordTimestamp = null;
        if (t2t.ttlColumn() != null) {
            recordTtl = row.getInt(t2t.ttlColumn().columnName());
        }
        if (t2t.timestampColumn() != null) {
            recordTimestamp = row.getLong(t2t.timestampColumn().columnName());
        }
        for (Map.Entry<Column, Column> entry: column2Column.entrySet()) {
            Column sourceColumn = entry.getKey();
            Column targetColumn = entry.getValue();
            String targetType = entry.getValue().columnType();
            String sClmName = entry.getKey().columnName();
            String tClmName = entry.getValue().columnName();
            switch (targetType) {
                case "tinyint" : {
                    byte v = row.getByte(sClmName);
                    if (targetColumn.isPartitionKey()) {
                        mapBytes.put(targetColumn.columnPosition(), byteToBytes(v));
                    }
                    objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), v));
                    break;
                }
                case "smallint" : {
                    short v = row.getShort(sClmName);
                    if (targetColumn.isPartitionKey()) {
                        mapBytes.put(targetColumn.columnPosition(), smallIntToBytes(v));
                    }
                    objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), v));
                    break;
                }
                case "int" : {
                    int v = row.getInt(sClmName);
                    if (targetColumn.isPartitionKey()) {
                        mapBytes.put(targetColumn.columnPosition(), intToBytes(v));
                    }
                    objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), v));
                    break;
                }
                case "bigint": {
                    long v = row.getLong(sClmName);
                    if (targetColumn.isPartitionKey()) {
                        mapBytes.put(targetColumn.columnPosition(), longToBytes(v));
                    }
                    objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), v));
                    break;
                }
                case "text": {
                    String v = row.getString(sClmName);
                    if (targetColumn.isPartitionKey() && v != null) {
                        mapBytes.put(targetColumn.columnPosition(), stringToBytes(v));
                    }
                    objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), v));
                    break;
                }
                case "date": {
                    LocalDate v = row.getLocalDate(sClmName);
                    objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), v));
                    break;
                }
                case "timestamp": {
                    Instant v = row.getInstant(sClmName);
                    if (targetColumn.isPartitionKey() && v != null) {
                        mapBytes.put(targetColumn.columnPosition(), timestampToBytes(v));
                    }
                    objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), v));
                    break;
                }
                case "boolean": {
                    Boolean v = row.getBoolean(sClmName);
                    objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), v));
                    break;
                }
                case "blob": {
                    ByteBuffer v = row.getByteBuffer(sClmName);
                    objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), v));
                    break;
                }
                case "float": {
                    Float v = row.getFloat(sClmName);
                    objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), v));
                    break;
                }
                case "decimal": {
                    BigDecimal v = row.getBigDecimal(sClmName);
                    objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), v));
                    break;
                }
                case "uuid": {
                    UUID v = row.getUuid(sClmName);
                    if (targetColumn.isPartitionKey() && v != null) {
                        mapBytes.put(targetColumn.columnPosition(), uuidToBytes(v));
                    }
                    objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), v));
                    break;
                }
                default:
                    Object v = row.getObject(sClmName);
                    objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), v));
                    break;
            }
        }
        byte[][] bytes = new byte[mapBytes.size()][];
        mapBytes.forEach((k, v) -> bytes[k] = v);
        TokenRange tokenRange = getTokenRange(tokenRangeSet, compositeToBytes(bytes));
        return new CSRecord(tokenRange, objectList, new CSValueAttribute(recordTtl, recordTimestamp));
    }

    public CSValue getCSValue(Integer recordTtl,
                              Long recordTimestamp,
                              Row row,
                              Column sourceColumn,
                              Column targetColumn,
                              Object value) {
        Integer ttl;
        Long timestamp;
        if (recordTtl == null && !sourceColumn.isStatic() && sourceColumn.columnPosition() == -1) {
//            System.out.println(sourceColumn.columnName() + " : " + value + " ttl : " + row.getInt("ttl(" + sourceColumn.columnName() + ")"));
            try {
                ttl = row.get("ttl(" + sourceColumn.columnName() + ")", Integer.class);
            } catch (CodecNotFoundException e) {
                ttl = null;
            }
        } else {
            ttl = recordTtl;
        }
        if (recordTimestamp == null && !sourceColumn.isStatic() && sourceColumn.columnPosition() == -1) {
            try {
                timestamp = row.getLong("writetime(" + sourceColumn.columnName() + ")");
            } catch (CodecNotFoundException e) {
                timestamp = null;
            }
        } else {
            timestamp = recordTimestamp;
        }
        return new CSValue(targetColumn, value, new CSValueAttribute(ttl, timestamp));
    }
}
