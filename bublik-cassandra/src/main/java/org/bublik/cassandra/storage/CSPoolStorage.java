package org.bublik.cassandra.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import org.bublik.cassandra.model.CSTable;
import org.bublik.cassandra.service.CSPoolStorageService;
import org.bublik.cassandra.service.CSTableService;
import org.bublik.cassandra.storage.cassandraaddons.BatchEntity;
import org.bublik.cassandra.storage.cassandraaddons.CSObject;
import org.bublik.cassandra.storage.cassandraaddons.CSPartitionKey;
import org.bublik.core.model.*;
import org.bublik.core.service.Sourceable;
import org.bublik.core.service.Targetable;
import org.bublik.core.storage.AutoColseableStorage;
import org.bublik.core.storage.Storage;
import org.bublik.core.storage.StorageClass;
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

import static org.bublik.cassandra.constants.SQLConstants.*;
import static org.bublik.cassandra.storage.cassandraaddons.MM3.*;

public class CSPoolStorage extends AutoColseableStorage implements CSPoolStorageService, Sourceable, Targetable {
    private static final Logger log = LoggerFactory.getLogger(CSPoolStorage.class);

    private final int batchSize;
    private final CSPool csPool;
    private final ConnectionProperty connectionProperty;

    public CSPoolStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        this.connectionProperty = connectionProperty;
        this.batchSize = getBatchSize(connectionProperty);
        this.csPool = new CSPool(getStorageClass().getProperties(), connectionProperty.getThreadCount());
    }

    public int getBatchSize(ConnectionProperty connectionProperty) {
        String batchSize = connectionProperty.getToProperty().getProperty("batchSize");
        return  batchSize == null ? 100 : Integer.parseInt(batchSize);
    }

    @Override
    public void start(List<Config> cfgs, boolean sync, int rows, Storage targetStorage, String tableName) throws SQLException {
        log.info("Cassandra target storage started");
        List<Config> configs = copyConfigs(cfgs);
        createChunkTable(sync, tableName);
        createChunks(configs, sync, rows, tableName);
//        dropChunkTable(sync, tableName);
    }

    private void createChunkTable(boolean sync, String tableName) {
        CqlSession cqlSession = csPool.getCqlSession();
        cqlSession.execute(DDL_CREATE_CHUNK_TABLE.replace("$tableName", getChunkTableName(tableName)));
    }

    @Override
    public LogMessage transferToTarget(Chunk<?, ?> chunk, String tableName) throws SQLException {
        return rangedBatch(chunk, tableName);
    }

    public LogMessage rangedBatch(Chunk<?, ?> chunk, String tableName) throws SQLException {
        int recordCount = 0;
        int batchCount = 0;
        long start = System.currentTimeMillis();
        CqlSession cqlSession = csPool.getCqlSession();
        if (isChunkProcessed(cqlSession, (int) chunk.getId(), chunk.getConfig().fromTaskName(), tableName)) {
            return new LogMessage(
                    0,
                    chunk.getStartTime(),
                    System.currentTimeMillis(),
                    "Chunk id = " + chunk.getId() + " already processed, skip it",
                    chunk);
        }
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
        insertProcessedChunkInfo(cqlSession, (int) chunk.getId(), recordCount, chunk.getConfig().fromTaskName(), tableName);
        long stop = System.currentTimeMillis();
        return new LogMessage(
                recordCount,
                start,
                stop,
                "Cassandra RANGED BATCH APPLY (batches: " + batchCount + ")",
                chunk);
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
//            log.error("Batch apply timeout: {}", e.getMessage());
            throw new SQLException(e);
        }
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
            String targetType = entry.getValue().columnType();
            switch (targetType) {
                case "smallint": {
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
                case "int": {
                    int v = resultSet.getInt(sourceColumn);
//                    temp = v;
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
                    UUID uuid = null;
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
        byte[][] bytes = new byte[mapBytes.size()][];
        mapBytes.forEach((k, v) -> bytes[k] = v);
        TokenRange tokenRange = getTokenRange(tokenRangeSet, compositeToBytes(bytes));
        return new AbstractMap.SimpleEntry<>(tokenRange, objectList.toArray());
    }

    @Override
    public void createChunks(List<Config> configs, boolean sync, int rows, String tableName) throws SQLException {
        CqlSession cqlSession = csPool.getCqlSession();
        Set<TokenRange> trs = csPool.getTokenRanges();

        for (Config c : configs) {
            Table pseudoTable = new PseudoTable(c.fromSchemaName(), c.fromTableName());
            List<Column> partitionKey = CSTableService.getKey(cqlSession, pseudoTable, "partition_key");
            List<Column> clusteringKey = CSTableService.getKey(cqlSession, pseudoTable, "clustering");
            CSTable sourceTable = new CSTable(pseudoTable.getSchemaName(), pseudoTable.getTableName(), partitionKey, clusteringKey);
            trs.forEach(tr -> {
//                UUID chunkId = Uuids.timeBased();
                long startValue = ((Murmur3Token) tr.getStart()).getValue();
                long stopValue = ((Murmur3Token) tr.getEnd()).getValue();
                if (stopValue > startValue) {
                    double estimatedRowsInRange = (long) getEstimatedRowsInRange(cqlSession, sourceTable, tr);
                    log.info("Estimated rows in range: {} - {} = {}", startValue, stopValue, estimatedRowsInRange);
                    if (estimatedRowsInRange == 0) {
                        log.info("Estimated rows in range: {} - {} = 0, skip chunk creation", startValue, stopValue);
                        return;
                    }
/*
                if (estimatedRowsInRange < rows) {
                    log.info("Estimated rows in range: {} - {} = {}, less than rows {}, skip chunk creation",
                            startValue, stopValue, estimatedRowsInRange, rows);
                    return;
                }
*/
                    long chunkCount = (long) Math.ceil(estimatedRowsInRange / rows);
                    long shift = (stopValue - startValue) / chunkCount;
                    long i = startValue;
                    PreparedStatement ps = cqlSession.prepare(DML_INSERT_CHUNK_TABLE.replace("$tableName", getChunkTableName(tableName)));
                    while ((i = i + shift) < stopValue) {
                        insertChunk(cqlSession, ps, i - shift, i, sourceTable, c.fromTaskName());
                    }
                    insertChunk(cqlSession, ps, i - shift, stopValue, sourceTable, c.fromTaskName());
                }
            });
        }
        log.info("Chunk table fulfilled successfully");
    }

    private void insertChunk(CqlSession cqlSession, PreparedStatement ps, long start, long stop, CSTable sourceTable, String taskName) {
        BoundStatement bs = ps.bind(
                start,
                stop,
                sourceTable.getSchemaName(),
                sourceTable.getTableName(),
                "UNASSIGNED",
                taskName);
        cqlSession.execute(bs);
    }

    private double getEstimatedRowsInRange(CqlSession cqlSession, CSTable sourceTable, TokenRange tr) {
        String queryCount = CSTableService.countRowsInTableQuery(sourceTable);
        PreparedStatement ps = cqlSession.prepare(queryCount);
        long startValue = ((Murmur3Token) tr.getStart()).getValue();
        long stopValue = ((Murmur3Token) tr.getEnd()).getValue();
        long delta = (stopValue - startValue) / 10;
        long g = delta / 10;
        long subRange = 0;
        long rowsInSubRange = 0;
        for (long j = startValue; j < startValue + delta; j = j + g ) {
            BoundStatement bsCount = ps.bind(j, j + g).setPageSize(10000);
            com.datastax.oss.driver.api.core.cql.ResultSet rs = cqlSession.execute(bsCount);
            long rCount = 0;
            for (Row row : rs) {
                rCount++;
            }
            subRange += g;
            rowsInSubRange += rCount;
        }
        return (double) Math.round((double) (stopValue - startValue) / subRange * rowsInSubRange);
    }

    @Override
    public void dropChunkTable(boolean sync, String tableName) throws SQLException {
        CqlSession cqlSession = csPool.getCqlSession();
        cqlSession.execute(DDL_DROP_TABLE.replace("$tableName", getChunkTableName(tableName)));
    }

    private String getChunkTableName(String tableName) {
        String[] t = tableName.split("\\.");
        String tmpName;
        if (t.length == 1) {
            tmpName = t[0];
        } else {
            tmpName = t[1];
        }
        String kSpace = "\"" + connectionProperty.getFromProperty().getProperty("keyspace") + "\"";
        return kSpace + "." + "\"" + tmpName + "\"";
    }

    private String getOutboxTableName(String tableName) {
        String[] t = tableName.split("\\.");
        String tmpName;
        if (t.length == 1) {
            tmpName = t[0];
        } else {
            tmpName = t[1];
        }
        String kSpace = "\"" + connectionProperty.getToProperty().getProperty("keyspace") + "\"";
        return kSpace + "." + "\"" + tmpName + "_outbox" + "\"";
    }

    @Override
    public void createOutbox(String tableName) throws SQLException {
        CqlSession cqlSession = csPool.getCqlSession();
        cqlSession.execute(DDL_CREATE_OUTBOX_TABLE.replace("$tableName", getOutboxTableName(tableName)));
        log.info("Outbox table created successfully");
    }

    public boolean isChunkProcessed(CqlSession cqlSession, int chunkId, String taskName, String tableName) throws SQLException {
        String selectCQL = DML_SELECT_OUTBOX_TABLE.replace("$tableName", getOutboxTableName(tableName));
        com.datastax.oss.driver.api.core.cql.ResultSet rs = cqlSession.execute(selectCQL, chunkId);
        return rs.one() != null;
    }

    public void insertProcessedChunkInfo(CqlSession cqlSession, int chunkId, int rows, String taskName, String tableName) throws SQLException {
        String insertCQL = DML_INSERT_OUTBOX_TABLE.replace("$tableName", getOutboxTableName(tableName));
        cqlSession.execute(insertCQL, chunkId, taskName, rows);
    }

    @Override
    public void dropOutboxTable(boolean sync, String tableName) throws SQLException {
        CqlSession cqlSession = csPool.getCqlSession();
        cqlSession.execute(DDL_DROP_TABLE.replace("$tableName", getOutboxTableName(tableName)));
    }

    @Override
    public List<Chunk<?, ?>> getChunkList(List<Config> configs, Connection connection, String chunkTable) throws SQLException {
        return List.of();
    }

/*
    @Override
    public Connection getPoolConnection() throws SQLException {
        return null;
    }
*/

    @Override
    public void closeStorage() {
        csPool.closeCqlSession();
        log.info("Cassandra target storage stopped");
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
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?> chunk) {
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

    @Override
    public void close() throws Exception {
        csPool.closeCqlSession();
    }

    @Override
    public List<Config> copyConfigs(List<Config> cfgs) {
        List<Config> configs = new ArrayList<>();
        for (Config c : cfgs) {
            configs.add(c.copy());
        }
        return configs;
    }
}
