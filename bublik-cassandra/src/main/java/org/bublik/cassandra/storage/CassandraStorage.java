package org.bublik.cassandra.storage;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.UserDefinedTypeParser;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bublik.cassandra.model.CSComplexType;
import org.bublik.cassandra.model.CSTable;
import org.bublik.cassandra.storage.cassandraaddons.*;
import org.bublik.core.model.*;
import org.bublik.core.storage.JDBCStorage;
import org.bublik.core.storage.Storage;
import org.bublik.core.storage.StorageClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

/*
    public LogMessage rangedByTokenRangeBatch(Chunk<K, T, S, R> chunk, ResultSet resultSet) throws SQLException {
        int recordCount = 0;
        int batchCount = 0;
        long start = System.currentTimeMillis();
        CqlSession cqlSession = chunk.getTargetSession();
        CSObject csObject = CSObject.createCSObject(getCsPool(), chunk);
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
*/

    public LogMessage rangedByTokenRangeBatch(Chunk<K, T, S, R> chunk, ResultSet resultSet) throws SQLException {
        int recordCount = 0;
        int batchCount = 0;
        long start = System.currentTimeMillis();
        CqlSession cqlSession = chunk.getTargetSession();

        Set<TokenRange> tokenRangeSet = getCsPool().tokenRanges();
        MM3Batch mm3Batch = MM3Batch.createMM3Batch();
        mm3Batch.initMM3Batch(tokenRangeSet);
        Map<TokenRange, BatchEntity> tokenRangeBatchEntityMap = mm3Batch.getTokenRangeMap();

        while (resultSet.next()) {
            CSRecord csRecord = getCSRecord(
                    resultSet,
                    chunk.getT2t(),
                    tokenRangeSet,
                    cqlSession);
            BatchEntity batchEntity = tokenRangeBatchEntityMap.get(csRecord.tokenRange());
            BatchStatementBuilder batchStatementBuilder = batchEntity.getBatchStatementBuilder();

            List<Object> objects = new ArrayList<>(csRecord.values().stream().map(CSValue::value).toList());
            Integer ttl = csRecord.attribute().ttl();
            Long timestamp = csRecord.attribute().timestamp();
            if (ttl != null) {
                objects.add(ttl);
            }
            if (timestamp != null) {
                objects.add(timestamp);
            }
            String insertQuery = csRecord.buildInsertStatement(chunk);
            PreparedStatement ps = cqlSession.prepare(insertQuery);
            BatchableStatement<?> statement = ps.bind(objects.toArray());
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
//        int batchRecordCount = 0;
        int batchCount = 0;
        long start = System.currentTimeMillis();
        CqlSession cqlSession = chunk.getTargetSession();
        Set<TokenRange> tokenRangeSet = getCsPool().tokenRanges();
        MM3Batch mm3Batch = MM3Batch.createMM3Batch();
        mm3Batch.initMM3Batch(tokenRangeSet);
        Map<TokenRange, BatchEntity> tokenRangeBatchEntityMap = mm3Batch.getTokenRangeMap();

        for (Row row : resultSet) {
            CSRecord csRecord = getCSRecord(row, chunk.getT2t(), tokenRangeSet);
//            System.out.println(csRecord);

            BatchEntity batchEntity = tokenRangeBatchEntityMap.get(csRecord.tokenRange());
            BatchStatementBuilder batchStatementBuilder = batchEntity.getBatchStatementBuilder();

            Map<CSValueAttribute, List<CSValue>> map = csRecord.values()
                    .stream()
                    .filter(CSValue::isRegular)
//                    .filter(CSValue::isNonStatic)
                    .collect(Collectors.groupingBy(CSValue::groupByAttribute));
//            map.forEach((k, v) -> System.out.println(k + " " + v));

            if (!map.isEmpty()) {
                for (Map.Entry<CSValueAttribute, List<CSValue>> entry : map.entrySet()) {
                    List<CSValue> csValues = new ArrayList<>(entry.getValue());
                    csValues.addAll(csRecord.values()
                                    .stream()
                                    .filter(CSValue::isNonRegular)
//                        .filter(CSValue::isStatic)
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
//                    System.out.println(insertQuery);
//                    System.out.println(objects);

                    PreparedStatement ps = cqlSession.prepare(insertQuery);
                    BatchableStatement<?> statement = ps.bind(objects.toArray());

                    batchStatementBuilder.addStatement(statement);
                    batchEntity.increaseCounter();
//                System.out.println("recordCount = " + batchRecordCount);
//                batchRecordCount++;

                    // batch_size_fail_threshold_in_kb: 50
                    if (batchEntity.getCounter() == batchSize) {
                        batchApply(batchStatementBuilder, cqlSession);
                        batchEntity.resetCounter();
                        batchCount++;
                    }

                }
            } else {
                String insertQuery = csRecord.buildInsertStatement(chunk);
                List<Object> objects = new ArrayList<>(csRecord.values().stream().map(CSValue::value).toList());
//                    System.out.println(insertQuery);
//                    System.out.println(objects);
                PreparedStatement ps = cqlSession.prepare(insertQuery);
                BatchableStatement<?> statement = ps.bind(objects.toArray());

                batchStatementBuilder.addStatement(statement);
                batchEntity.increaseCounter();
//                System.out.println("recordCount = " + batchRecordCount);
//                batchRecordCount++;

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

    private record CSObj(Object object, byte[] bytes) {
    }

    private <C1, C2> CSObj getCSObj(ResultSet resultSet, Column targetColumn, String sClmName) throws SQLException {
        Object v = null;
        byte[] bytes = null;
        String targetType = targetColumn.columnType();
        switch (targetType) {
            case "tinyint": {
                v = (byte) resultSet.getInt(sClmName);
                if (targetColumn.isPartitionKey()) {
                    bytes = byteToBytes((byte) v);
                }
                break;
            }
            case "smallint": {
                v = resultSet.getShort(sClmName);
                if (targetColumn.isPartitionKey()) {
                    bytes = smallIntToBytes((short) v);
                }
                break;
            }
            case "int": {
                v = resultSet.getInt(sClmName);
                if (targetColumn.isPartitionKey()) {
                    bytes = intToBytes((Integer) v);
                }
                break;
            }
            case "bigint": {
                v = resultSet.getLong(sClmName);
                if (targetColumn.isPartitionKey()) {
                    bytes = longToBytes((Long) v);
                }
                break;
            }
            case "text": {
                v = resultSet.getString(sClmName);
                if (targetColumn.isPartitionKey() && v != null) {
                    bytes = stringToBytes((String) v);
                }
                break;
            }
            case "date": {
                Timestamp t = resultSet.getTimestamp(sClmName);
                long l = t.getTime();
                v = Instant.ofEpochMilli(l).atZone(ZoneId.systemDefault()).toLocalDate();
                break;
            }
            case "timestamp": {
                v = resultSet.getTimestamp(sClmName).toInstant();
                if (targetColumn.isPartitionKey() && v != null) {
                    bytes = timestampToBytes((Instant) v);
                }
                break;
            }
            case "boolean": {
                v = resultSet.getBoolean(sClmName);
                break;
            }
            case "blob": {
                byte[] bs = resultSet.getBytes(sClmName);
                if (bs != null) {
                    v = ByteBuffer.wrap(bs);
                }
                break;
            }
            case "float": {
                v = resultSet.getFloat(sClmName);
                break;
            }
            case "decimal": {
                v = resultSet.getBigDecimal(sClmName);
                break;
            }
            case "uuid": {
                Object tmp = resultSet.getObject(sClmName);
                try {
                    v = tmp;
                } catch (ClassCastException e) {
                    v = UUID.fromString((String) tmp);
                }
                if (targetColumn.isPartitionKey() && v != null) {
                    bytes = uuidToBytes((UUID) v);
                }
                break;
            }
            default:
                String tmp;
                Pattern pattern = Pattern.compile("(frozen)<(.*)>>");
                Matcher matcher = pattern.matcher(targetType);
                if (targetType.contains("frozen") && matcher.find()) {
                    tmp = targetType.substring(7, targetType.length() - 1);
                } else {
                    tmp = targetType;
                }
                CSComplexType<?> complexType = CSComplexType.of(tmp);
                ObjectMapper mapper = new ObjectMapper();
                mapper.enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES);
                switch (complexType.typeName()) {
                    case "list": {
                        String s = resultSet.getString(sClmName);
                        try {
                            Class<C1> c1 = (Class<C1>) complexType.fieldTypes().getFirst();
                            v = getListOf(c1);
                            ((List<?>) v).addAll(mapper.readValue(s, List.class));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    }
                    case "set": {
                        String s = resultSet.getString(sClmName);
                        try {
                            Class<C1> c1 = (Class<C1>) complexType.fieldTypes().getFirst();
                            v = getSetOf(c1);
                            ((Set<?>) v).addAll(mapper.readValue(s, Set.class));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    }
                    case "map": {
                        String s = resultSet.getString(sClmName);
                        try {
                            Class<C1> c1 = (Class<C1>) complexType.fieldTypes().getFirst();
                            Class<C2> c2 = (Class<C2>) complexType.fieldTypes().getLast();
                            if (c1 == Integer.class && c2 == String.class) {
                                v = mapper.readValue(s, new TypeReference<HashMap<Integer, String>>() {
                                });
                            } else if (c1 == String.class && c2 == String.class) {
                                v = mapper.readValue(s, new TypeReference<HashMap<String, String>>() {
                                });
                            } else if (c1 == String.class && c2 == Integer.class) {
                                v = mapper.readValue(s, new TypeReference<HashMap<String, Integer>>() {
                                });
                            } else if (c1 == Integer.class && c2 == Integer.class) {
                                v = mapper.readValue(s, new TypeReference<HashMap<Integer, Integer>>() {
                                });
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    }
                    default:
                        break;
                }
                break;
        }
        return new CSObj(v, bytes);
    }

    private CSRecord getCSRecord(ResultSet resultSet,
                                 Table2Table<?> t2t,
                                 Set<TokenRange> tokenRangeSet,
                                 CqlSession cqlSession) throws SQLException {
        List<CSValue> objectList = new ArrayList<>();
        Map<Column, Column> column2Column = new HashMap<>();
        t2t.column2Columns()
                .stream()
                .filter(e -> e.sourceColumn() != null)
                .forEach((c) -> column2Column.put(c.sourceColumn(), c.targetColumn()));
        Map<Integer, byte[]> mapBytes = new TreeMap<>();
        Integer ttl = null;
        Long timestamp = null;
        if (t2t.ttlColumn() != null) {
            ttl = resultSet.getInt(t2t.ttlColumn().columnName());
        }
        if (t2t.timestampColumn() != null) {
            timestamp = resultSet.getLong(t2t.timestampColumn().columnName());
        }
        for (Map.Entry<Column, Column> entry : column2Column.entrySet()) {
            Column sourceColumn = entry.getKey();
            Column targetColumn = entry.getValue();
            String sClmName = sourceColumn.columnName();
            CSObj csObj = getCSObj(resultSet, targetColumn, sClmName);
            objectList.add(new CSValue(targetColumn, csObj.object, null));
            if (targetColumn.isPartitionKey()) {
                mapBytes.put(targetColumn.columnPosition(), csObj.bytes);
            }
        }

        byte[][] bytes = new byte[mapBytes.size()][];
        mapBytes.forEach((k, v) -> bytes[k] = v);
        TokenRange tokenRange = getTokenRange(tokenRangeSet, compositeToBytes(bytes));

        Map<List<String>, Column> columns2List = new HashMap<>();
        t2t.column2Columns()
                .stream()
                .filter(e -> e.sourceColumn() == null && e.asList() != null)
                .forEach((c) -> columns2List.put(c.asList(), c.targetColumn()));

        for (Map.Entry<List<String>, Column> entry : columns2List.entrySet()) {
            List<String> sourceExprs = entry.getKey();
            Column targetColumn = entry.getValue();
            List<Object> objects = new ArrayList<>();
            for (String sourceExpr : sourceExprs) {
                String rsColName = sourceExpr.substring(sourceExpr.toLowerCase().lastIndexOf(" as ") + 4);
                objects.add(resultSet.getObject(rsColName));
            }
            objectList.add(new CSValue(targetColumn, objects, null));
        }

        Map<List<String>, Column> columns2Set = new HashMap<>();
        t2t.column2Columns()
                .stream()
                .filter(e -> e.sourceColumn() == null && e.asSet() != null)
                .forEach((c) -> columns2Set.put(c.asSet(), c.targetColumn()));

        for (Map.Entry<List<String>, Column> entry : columns2Set.entrySet()) {
            List<String> sourceExprs = entry.getKey();
            Column targetColumn = entry.getValue();
            Set<Object> objects = new HashSet<>();
            for (String sourceExpr : sourceExprs) {
                String rsColName = sourceExpr.substring(sourceExpr.toLowerCase().lastIndexOf(" as ") + 4);
                objects.add(resultSet.getObject(rsColName));
            }
            objectList.add(new CSValue(targetColumn, objects, null));
        }

        Map<List<KV>, Column> columns2Map = new HashMap<>();
        t2t.column2Columns()
                .stream()
                .filter(e -> e.sourceColumn() == null && e.asMap() != null)
                .forEach((c) -> columns2Map.put(c.asMap(), c.targetColumn()));

        for (Map.Entry<List<KV>, Column> entry : columns2Map.entrySet()) {
            List<KV> sourceExprs = entry.getKey();
            Column targetColumn = entry.getValue();
            Map<Object, Object> objects = new HashMap<>();
            for (KV sourceExpr : sourceExprs) {
                String rsColNameKey = sourceExpr.key().substring(sourceExpr.key().toLowerCase().lastIndexOf(" as ") + 4);
                String rsColNameValue = sourceExpr.value().substring(sourceExpr.value().toLowerCase().lastIndexOf(" as ") + 4);
                Object key = resultSet.getObject(rsColNameKey);
                Object value = resultSet.getObject(rsColNameValue);
//                log.info("targetColumn: {}, rsColNameKey: {} - {}, rsColNameValue: {} - {}", targetColumn.columnName(), rsColNameKey, key, rsColNameValue, value);
                objects.put(key, value);
            }
            objectList.add(new CSValue(targetColumn, objects, null));
        }

        Map<List<String>, Column> columns2UDT = new HashMap<>();
        t2t.column2Columns()
                .stream()
                .filter(e -> e.sourceColumn() == null && e.asUDT() != null)
                .forEach((c) -> columns2UDT.put(c.asUDT(), c.targetColumn()));
        List<CSTable.UDTColumn> udtColumns = ((CSTable)t2t.targetTable()).getUdtColumns();

        for (Map.Entry<List<String>, Column> entry : columns2UDT.entrySet()) {
            List<String> sourceExprs = entry.getKey();
            Column targetColumn = entry.getValue();
            List<Column> typeColumns = targetColumn.udtType().columns();
            UserDefinedType udt = null;
            for (CSTable.UDTColumn udtColumn : udtColumns) {
                if (udtColumn.column().columnName().equals(targetColumn.columnName())) {
                    udt = udtColumn.udt();
                }
            }
            List<Object> objects = new ArrayList<>();
            int i = 0;
            for (String sourceExpr : sourceExprs) {
                String rsColName = sourceExpr.substring(sourceExpr.toLowerCase().lastIndexOf(" as ") + 4);
                Column typeColumn = typeColumns.get(i);
                CSObj csObj = getCSObj(resultSet, typeColumn, rsColName);
                objects.add(csObj.object);
                i++;
            }
            assert udt != null;
            UdtValue v = udt.newValue(objects.toArray());
            objectList.add(new CSValue(targetColumn, v, null));
        }

        return new CSRecord(tokenRange, objectList, new CSValueAttribute(ttl, timestamp));
    }

    private <C> List<C> getListOf(Class<C> c) {
        return new ArrayList<>();
    }

    private <C> Set<C> getSetOf(Class<C> c) {
        return new HashSet<>();
    }

    private CSObj getCSObj(Row row, Column targetColumn, String sClmName) throws SQLException {
        Object v;
        byte[] bytes = null;
        String targetType = targetColumn.columnType();
        switch (targetType) {
            case "tinyint" : {
                v = row.getByte(sClmName);
                if (targetColumn.isPartitionKey()) {
                    bytes = byteToBytes((Byte) v);
                }
                break;
            }
            case "smallint" : {
                v = row.getShort(sClmName);
                if (targetColumn.isPartitionKey()) {
                    bytes = smallIntToBytes((Short) v);
                }
                break;
            }
            case "int" : {
                v = row.getInt(sClmName);
                if (targetColumn.isPartitionKey()) {
                    bytes = intToBytes((Integer) v);
                }
                break;
            }
            case "bigint": {
                v = row.getLong(sClmName);
                if (targetColumn.isPartitionKey()) {
                    bytes = longToBytes((Long) v);
                }
                break;
            }
            case "text": {
                v = row.getString(sClmName);
                if (targetColumn.isPartitionKey() && v != null) {
                    bytes = stringToBytes((String) v);
                }
                break;
            }
            case "date": {
                v = row.getLocalDate(sClmName);
                break;
            }
            case "timestamp": {
                v = row.getInstant(sClmName);
                if (targetColumn.isPartitionKey() && v != null) {
                    bytes = timestampToBytes((Instant) v);
                }
                break;
            }
            case "boolean": {
                v = row.getBoolean(sClmName);
                break;
            }
            case "blob": {
                v = row.getByteBuffer(sClmName);
                break;
            }
            case "float": {
                v = row.getFloat(sClmName);
                break;
            }
            case "decimal": {
                v = row.getBigDecimal(sClmName);
                break;
            }
            case "uuid": {
                v = row.getUuid(sClmName);
                if (targetColumn.isPartitionKey() && v != null) {
                    bytes = uuidToBytes((UUID) v);
                }
                break;
            }
            default:
                v = row.getObject(sClmName);
                break;
        }
//        System.out.println("v = " + v);
//        System.out.println("bytes = " + bytes);
        return new CSObj(v, bytes);
    }

    private CSRecord getCSRecord(Row row, Table2Table<S> t2t, Set<TokenRange> tokenRangeSet) throws SQLException {
        List<CSValue> objectList = new ArrayList<>();
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
            String sClmName = entry.getKey().columnName();
            CSObj csObj = getCSObj(row, targetColumn, sClmName);
            objectList.add(getCSValue(recordTtl, recordTimestamp, row, sourceColumn, entry.getValue(), csObj.object()));
            if (targetColumn.isPartitionKey() && csObj.object() != null) {
                mapBytes.put(targetColumn.columnPosition(), csObj.bytes());
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
