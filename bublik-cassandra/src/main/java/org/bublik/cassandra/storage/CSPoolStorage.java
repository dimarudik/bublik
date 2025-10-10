package org.bublik.cassandra.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import org.bublik.cassandra.service.CSPoolStorageService;
import org.bublik.cassandra.storage.cassandraaddons.BatchEntity;
import org.bublik.cassandra.storage.cassandraaddons.CSObject;
import org.bublik.cassandra.storage.cassandraaddons.CSPartitionKey;
import org.bublik.core.model.*;
import org.bublik.core.storage.AutoColseableStorage;
import org.bublik.core.storage.Storage;
import org.bublik.core.storage.StorageClass;
import org.bublik.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static org.bublik.cassandra.storage.cassandraaddons.MM3.*;
import static org.bublik.cassandra.storage.cassandraaddons.MM3.compositeToBytes;
import static org.bublik.cassandra.storage.cassandraaddons.MM3.getTokenRange;
import static org.bublik.cassandra.storage.cassandraaddons.MM3.stringToBytes;
import static org.bublik.cassandra.storage.cassandraaddons.MM3.timestampToBytes;
import static org.bublik.cassandra.storage.cassandraaddons.MM3.uuidToBytes;

public class CSPoolStorage extends AutoColseableStorage implements CSPoolStorageService {
    private static final Logger log = LoggerFactory.getLogger(CSPoolStorage.class);
    private static final ReentrantLock lock = new ReentrantLock();

    private final int batchSize;
    private final CSPool csPool;
//    private final Map<CqlSession, Short> sessionMap;

    public CSPoolStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        this.batchSize = getBatchSize(connectionProperty);
        this.csPool = new CSPool(getStorageClass().getProperties(), connectionProperty.getThreadCount());

//        sessionMap = initSessionMap(connectionProperty.getThreadCount());
/*
        try {
            Thread.sleep(30_000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
*/
//        closeStorage();
    }

    public int getBatchSize(ConnectionProperty connectionProperty) {
        String batchSize = connectionProperty.getToProperty().getProperty("batchSize");
        return  batchSize == null ? 100 : Integer.parseInt(batchSize);
    }

/*
    public Map<CqlSession, Short> getSessionMap() {
        return sessionMap;
    }
*/

/*
    @Override
    public Map<CqlSession, Short> initSessionMap(int threadCount) {
        Map<CqlSession, Short> sessionMap = new ConcurrentHashMap<>();
        for (short i = 0; i < threadCount; i++) {
            sessionMap.put(createCqlSession(), (short) 0);
        }
        return sessionMap;
    }
*/


/*
    @Override
    public CqlSession getCqlSession() {
        CqlSession cqlSession;
        lock.lock();
        try {
             cqlSession = getSessionMap()
                    .entrySet()
                    .stream()
                    .filter(e -> e.getValue() == 0)
                    .findFirst()
                    .map(this::lockSession)
                    .map(Map.Entry::getKey)
                    .orElseThrow();
//            getSessionMap().put(cqlSession, (short) 1);
        } finally {
            lock.unlock();
        }
        return cqlSession;
    }
*/

/*
    private Map.Entry<CqlSession, Short> lockSession(Map.Entry<CqlSession, Short> entry) {
        entry.setValue((short) 1);
        return entry;
    }
*/


/*
    @Override
    public void freeCqlSession(CqlSession cqlSession) {
        getSessionMap().put(cqlSession, (short) 0);
    }
*/


    @Override
    public void start(List<Config> configs, boolean sync, int rows, Storage targetStorage, String tableName) throws SQLException {

    }

    @Override
    public void createChunks(Connection connection, List<Config> configs, boolean sync, int rows, String tableName) throws SQLException {

    }

    @Override
    public void dropChunkTable(Connection connection, boolean sync, String tableName) throws SQLException {

    }

    @Override
    public void createOutbox(String tableName) throws SQLException {

    }

    @Override
    public void insertProcessedChunkInfo(Connection connection, int chunkId, int rows, String taskName, String tableName) throws SQLException {

    }

    @Override
    public void dropOutboxTable(boolean sync, String tableName) throws SQLException {

    }

    @Override
    public List<Chunk<?>> getChunkList(List<Config> configs, Connection connection, String chunkTable) throws SQLException {
        return List.of();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public LogMessage transferToTarget(Chunk<?> chunk, String tableName) throws SQLException {
        LogMessage logMessage = rangedBatch(chunk);
//        Foo();
        return logMessage;
    }

    private void Foo() {
        try {
            CqlSession cqlSession = csPool.getCqlSession();
            cqlSession.close();
        } catch (Exception e) {
            log.error("{}", Utils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    public LogMessage rangedBatch(Chunk<?> chunk) throws SQLException {
        int recordCount = 0;
        int batchCount = 0;
        long start = System.currentTimeMillis();
        CqlSession cqlSession = csPool.getCqlSession();
        CSObject csObject = CSObject.createCSObject(cqlSession, chunk);
        ResultSet resultSet = chunk.getResultSet();
        while (resultSet.next()) {
            Map.Entry<TokenRange, Object[]> entry = getTokenRangedObjects(
                    resultSet,
                    csObject.getPartitionKeyMap(),
                    csObject.getCassandraColumnMap(),
                    csObject.getTokenRangeSet());
            Map<TokenRange, BatchEntity> tokenRangeBatchEntityMap = csObject.getMm3Batch().getTokenRangeMap();
            BatchEntity batchEntity = tokenRangeBatchEntityMap.get(entry.getKey());
            BatchStatementBuilder batchStatementBuilder = batchEntity.getBatchStatementBuilder();
            batchStatementBuilder.addStatement(csObject.getPreparedStatement().bind(entry.getValue()));
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
//        cqlSession.close();
        long stop = System.currentTimeMillis();
        return new LogMessage(
                recordCount,
                start,
                stop,
                "Cassandra RANGED BATCH APPLY (batches: " + batchCount + ")",
                chunk);
    }

    private void batchApply(BatchStatementBuilder batchStatementBuilder, CqlSession cqlSession) {
        BatchStatement batchStatement = batchStatementBuilder
                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                .build();
        cqlSession.execute(batchStatement);
/*
        cqlSession
                .executeAsync(batchStatement)
                .whenComplete((asyncResultSet, throwable) -> {
                    if (throwable != null) {
                        log.info("{}", Utils.getStackTrace(throwable));
                    }
                });
*/
        batchStatementBuilder.clearStatements();
        batchStatement.clear();
    }

    @Override
    public void closeStorage() {
        csPool.closeCqlSession();
    }

    @Override
    public String buildFetchStatement(Config config, Table sourceTable) {
        return buildFetchStatement(config);
    }

    @Override
    public String buildFetchStatement(Config config) {
        return "";
    }

    @Override
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?> chunk) {
        return Map.of();
    }

    @Override
    public Map<Table, Table> configsToTables(List<Config> configs, Storage targetStorage) {
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

    @Override
    public <T> T unwrap(Class<T> iface) {
        if (iface.isInstance(this)) {
            return (T) this;
        } else {
            throw new RuntimeException("No object found that implements the interface: " + iface.getName());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    private Map.Entry<TokenRange, Object[]> getTokenRangedObjects(ResultSet resultSet,
                                                                  Map<Integer, CSPartitionKey> partitionKeyMap,
                                                                  Map<String, Column> stringCassandraColumnMap,
                                                                  Set<TokenRange> tokenRangeSet) throws SQLException {
        List<Object> objectList = new ArrayList<>();
        Map<Integer, byte[]> mapBytes = new TreeMap<>();
//        long temp = 0;
        for (Map.Entry<String, Column> entry : stringCassandraColumnMap.entrySet()) {
            String sourceColumn = entry.getKey().replaceAll("\"", "");
            String targetType = entry.getValue().getColumnType();
            switch (targetType) {
                case "smallint": {
                    short v = resultSet.getShort(sourceColumn);
                    partitionKeyMap
                            .entrySet()
                            .stream()
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().getColumnName()))
                            .findFirst()
                            .ifPresent(e -> mapBytes.put(e.getKey(), smallIntToBytes(v)));
                    objectList.add(v);
                    break;
                }
                case "int": {
                    int v = resultSet.getInt(sourceColumn);
//                    temp = v;
                    partitionKeyMap
                            .entrySet()
                            .stream()
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().getColumnName()))
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
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().getColumnName()))
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
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().getColumnName()))
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
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().getColumnName()))
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
                    UUID uuid = null;
                    try {
                        uuid = (UUID) v;
                    } catch (ClassCastException e) {
                        uuid = UUID.fromString((String) v);
                    }
                    Map.Entry<Integer, CSPartitionKey> keyEntry = partitionKeyMap
                            .entrySet()
                            .stream()
                            .filter(e -> e.getValue().getColumnName().equals(entry.getValue().getColumnName()))
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
}
