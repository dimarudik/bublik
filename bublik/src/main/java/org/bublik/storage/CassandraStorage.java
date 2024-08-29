package org.bublik.storage;


import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import org.bublik.constants.PGKeywords;
import org.bublik.model.*;
import org.bublik.storage.cassandraaddons.BatchEntity;
import org.bublik.storage.cassandraaddons.MM3;
import org.bublik.storage.cassandraaddons.MM3Batch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
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
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static org.bublik.exception.Utils.getStackTrace;

public class CassandraStorage extends Storage {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStorage.class);
    private final Metadata metadata;
    private final int batchSize;
    private final CqlSession cqlSession;
    private final Set<TokenRange> tokenRangeSet;

    public CassandraStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        Properties properties = getStorageClass().getProperties();
        DriverConfigLoader configLoader = DriverConfigLoader
                .programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(Long.parseLong(properties.getProperty("query_time_out"))))
//                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(20))
                .build();
        List<String> hosts = Arrays.asList(getStorageClass().getProperties().getProperty("hosts").split(",", -1));
        List<InetSocketAddress> addresses = hosts
                .stream()
                .map(h -> new InetSocketAddress(h, Integer.parseInt(properties.getProperty("port"))))
                .toList();
        cqlSession = CqlSession
                .builder()
                .addContactPoints(addresses)
                .withConfigLoader(configLoader)
                .withAuthCredentials(properties.getProperty("user"), properties.getProperty("password"))
                .withLocalDatacenter(properties.getProperty("datacenter"))
                .build();
//        System.out.println(cqlSession.hashCode());
        this.metadata = cqlSession.getMetadata();
        this.tokenRangeSet = metadata.getTokenMap().orElseThrow().getTokenRanges();
//        tokenRangeSet.forEach(tokenRange -> System.out.println(((Murmur3Token)tokenRange.getStart()).getValue() + " : " +
//        ((Murmur3Token)tokenRange.getEnd()).getValue()));
//        System.out.println();
/*
        metadata
                .getNodes()
                .forEach((key, value) -> {
                    metadata.getTokenMap().orElseThrow().getTokenRanges(value)
                            .forEach(t -> System.out.println(((Murmur3Token)t.getStart()).getValue() + " : " + ((Murmur3Token)t.getEnd()).getValue()));
                    System.out.println();
                });
*/
        this.batchSize = getBatchSize(connectionProperty);
    }

    @Override
    public void start(List<Config> configs) throws SQLException {

    }

/*
    @Override
    public boolean hook(List<Config> configs) throws SQLException {
        return false;
    }
*/

    @Override
    public Map<Integer, Chunk<?>> getChunkMap(List<Config> configs) throws SQLException {
        return Map.of();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public LogMessage transferToTarget(Chunk<?> chunk) throws SQLException {
//        return simpleInsert(chunk);
//        return simpleBatch(chunk);
        return rangedBatch(chunk);
    }

    public LogMessage simpleInsert(Chunk<?> chunk) throws SQLException {
        int recordCount = 0;
        long start = System.currentTimeMillis();
        Map<String, CassandraColumn> stringCassandraColumnMap = readTargetColumnsAndTypes(chunk);
        String insertString = buildInsertStatement(chunk, stringCassandraColumnMap);
        PreparedStatement preparedStatement = cqlSession.prepare(insertString);
        ResultSet resultSet = chunk.getResultSet();
        while (resultSet.next()) {
            Map.Entry<TokenRange, Object[]> entry = getPreparedStatementObjects(resultSet, stringCassandraColumnMap);
            cqlSession.execute(preparedStatement.bind(entry.getValue()));
            recordCount++;
        }
        long stop = System.currentTimeMillis();
        return new LogMessage(
                recordCount,
                start,
                stop,
                "Cassandra SIMPLE INSERT",
                chunk);
    }

    public LogMessage simpleBatch(Chunk<?> chunk) throws SQLException {
        int recordCount = 0;
        long start = System.currentTimeMillis();
        Map<String, CassandraColumn> stringCassandraColumnMap = readTargetColumnsAndTypes(chunk);
        String insertString = buildInsertStatement(chunk, stringCassandraColumnMap);
        BatchStatementBuilder batchStatementBuilder = BatchStatement.builder(DefaultBatchType.LOGGED);
        PreparedStatement preparedStatement = cqlSession.prepare(insertString);
        ResultSet resultSet = chunk.getResultSet();
        List<CompletionStage<AsyncResultSet>> stages = new ArrayList<>();
        while (resultSet.next()) {
            Map.Entry<TokenRange, Object[]> entry = getPreparedStatementObjects(resultSet, stringCassandraColumnMap);
            batchStatementBuilder.addStatement(preparedStatement.bind(entry.getValue()));
            recordCount++;
            // batch_size_fail_threshold_in_kb: 50
            if (recordCount % batchSize == 0) {
                stages.add(batchApply(batchStatementBuilder));
            }
        }
        if (recordCount > 0) {
            stages.add(batchApply(batchStatementBuilder));
        }
        long stop = System.currentTimeMillis();
        return new LogMessage(
                recordCount,
                start,
                stop,
                "Cassandra SIMPLE BATCH APPLY",
                chunk);
    }

    public LogMessage rangedBatch(Chunk<?> chunk) throws SQLException {
        int recordCount = 0;
        long start = System.currentTimeMillis();
        try {
            MM3Batch mm3Batch = MM3Batch.initMM3Batch(tokenRangeSet);
            Map<String, CassandraColumn> stringCassandraColumnMap = readTargetColumnsAndTypes(chunk);
            String insertString = buildInsertStatement(chunk, stringCassandraColumnMap);
            PreparedStatement preparedStatement = cqlSession.prepare(insertString);
            ResultSet resultSet = chunk.getResultSet();
            List<CompletionStage<AsyncResultSet>> stages = new ArrayList<>();
            while (resultSet.next()) {
                Map.Entry<TokenRange, Object[]> entry = getPreparedStatementObjects(resultSet, stringCassandraColumnMap);
                Map<TokenRange, BatchEntity> tokenRangeBatchEntityMap = mm3Batch.getTokenRangeMap();
                BatchEntity batchEntity = tokenRangeBatchEntityMap.get(entry.getKey());
                BatchStatementBuilder batchStatementBuilder = batchEntity.getBatchStatementBuilder();
                batchStatementBuilder.addStatement(preparedStatement.bind(entry.getValue()));
                batchEntity.increaseCounter();
                recordCount++;
                // batch_size_fail_threshold_in_kb: 50
                if (batchEntity.getCounter() == batchSize) {
                    CompletionStage<AsyncResultSet> stage = batchApply(batchStatementBuilder);
                    stages.add(stage);
                    batchEntity.resetCounter();
                }
            }
            for (Map.Entry<TokenRange, BatchEntity> entry : mm3Batch.getTokenRangeMap().entrySet()) {
                if (entry.getValue().getCounter() > 0) {
//                System.out.println(entry.getValue().getCounter());
                    stages.add(batchApply(entry.getValue().getBatchStatementBuilder()));
                }
            }
//        mm3Batch.getTokenRangeMap().forEach((k, v) -> System.out.println(k + ":" + v.getCounter()));
//        System.out.println(mm3Batch.getMaxBatchEntity().getCounter());
        } catch (Exception e) {
            LOGGER.error("{}", getStackTrace(e));
        }
        long stop = System.currentTimeMillis();
        return new LogMessage(
                recordCount,
                start,
                stop,
                "Cassandra RANGED BATCH APPLY",
                chunk);
    }

/*
    public LogMessage rangedExecutorBatch(Chunk<?> chunk) throws SQLException {
        int threads = tokenRangeSet.size() + 1;
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        MM3Batch mm3Batch = MM3Batch.initMM3Batch(tokenRangeSet);
//        tokenRangeSet.forEach(tokenRange -> System.out.println(tokenRange.getStart() + " " + tokenRange.getEnd()));
        int recordCount = 0;
        long start = System.currentTimeMillis();
        try {
            Map<String, CassandraColumn> stringCassandraColumnMap = readTargetColumnsAndTypes(chunk);
            String insertString = buildInsertStatement(chunk, stringCassandraColumnMap);
            BatchStatementBuilder batchStatementBuilder = BatchStatement.builder(DefaultBatchType.LOGGED);
            PreparedStatement preparedStatement = cqlSession.prepare(insertString);
            ResultSet resultSet = chunk.getResultSet();
            while (resultSet.next()) {
                Object[] objects = getPreparedStatementObjects(resultSet, stringCassandraColumnMap, mm3Batch);
                batchStatementBuilder.addStatement(preparedStatement.bind(objects));
                recordCount++;
                // batch_size_fail_threshold_in_kb: 50
                if (recordCount % batchSize == 0) {
                    int r = recordCount;
                    BatchStatementBuilder builder = BatchStatement.builder(batchStatementBuilder.build());
                    batchStatementBuilder.clearStatements();
                    executorService.submit(() -> {
                        batchApply(builder, r);
                    });
//                    batchApply(batchStatementBuilder, recordCount);
                }
            }
            mm3Batch.getTokenRangeMap().forEach((k, v) -> System.out.println(k + ":" + v));
            if (recordCount > 0){
                batchApply(batchStatementBuilder, recordCount);
            }
        } catch (Exception e) {
            LOGGER.error("{}", e.getMessage());
            for (Throwable t : e.getSuppressed()) {
                LOGGER.error("{}", t.getMessage());
            }
        }
        long stop = System.currentTimeMillis();
        executorService.shutdown();
        executorService.close();
        return new LogMessage(
                recordCount,
                start,
                stop,
                "Cassandra EXECUTOR RANGED BATCH APPLY",
                chunk);
    }
*/

    private CompletionStage<AsyncResultSet> batchApply(BatchStatementBuilder batchStatementBuilder) {
        try {
            BatchStatement batchStatement = batchStatementBuilder
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .setTimeout(Duration.ofSeconds(60))
                    .build();
            Function<Throwable, AsyncResultSet> function = t -> {
                switch (t) {
                    case WriteTimeoutException ignored -> LOGGER.error("TIMEOUT {}", getStackTrace(t));
                    case InvalidQueryException ignored ->
                            LOGGER.error("batchSize is {} - {}", batchStatement.size(), getStackTrace(t));
                    case WriteFailureException ignored -> LOGGER.error("FAILURE {}", getStackTrace(t));
                    case null, default -> {
                        assert t != null;
                        LOGGER.error("{}", getStackTrace(t));
                    }
                }
                return null;
            };
//            CompletionStage<AsyncResultSet> stage = null;
            CompletionStage<AsyncResultSet> stage = cqlSession.executeAsync(batchStatement).exceptionally(function);
//            LOGGER.info("{} BATCHES APPLIED", batchStatement.size());
            batchStatementBuilder.clearStatements();
            batchStatement.clear();
            return stage;
        } catch (Exception e) {
            LOGGER.error("{}", getStackTrace(e));
            return null;
        }
    }

    private Map.Entry<TokenRange, Object[]> getPreparedStatementObjects(ResultSet resultSet,
                                                 Map<String, CassandraColumn> stringCassandraColumnMap) throws SQLException {
        // http://www.java2s.com/example/java-api/org/apache/cassandra/dht/murmur3partitioner/murmur3partitioner-0-0.html
        List<Object> objectList = new ArrayList<>();
        TokenRange tokenRange = null;
        for (Map.Entry<String, CassandraColumn> entry : stringCassandraColumnMap.entrySet()) {
            String sourceColumn = entry.getKey().replaceAll("\"", "");
            String targetType = entry.getValue().getColumnType();
            switch (targetType) {
                case "int": {
                    int v = resultSet.getInt(sourceColumn);
                    if (entry.getValue().getColumnName().equals("id")) {
                        MM3 mm3 = new MM3(v);
                        tokenRange = mm3.getTokenRange(tokenRangeSet);
//                        System.out.println(v + ";" + ((Murmur3Token)tokenRange.getStart()).getValue() + ";" +
//                                ((Murmur3Token)tokenRange.getEnd()).getValue() + ";" + mm3.getMurmur3Token().getValue());

//                        BatchEntity batchEntity = mm3Batch.getBatchEntity(tokenRange);
//                        batchEntity.increaseCounter();
//                        mm3Batch.putTokenRange(tokenRange, batchEntity);
//                        System.out.println("Value: " + v + " ; Token: " + mm3.getMurmur3Token().getValue() +
//                                " ; TokenRange: " + ((Murmur3Token)tokenRange.getStart()).getValue() + " - " +
//                                ((Murmur3Token)tokenRange.getEnd()).getValue());
                    }
                    objectList.add(v);
//                    objectList.add(1);
                    break;
                }
                case "bigint": {
                    objectList.add(resultSet.getLong(sourceColumn));
                    break;
                }
                case "smallint": {
                    objectList.add(resultSet.getShort(sourceColumn));
                    break;
                }
                case "text": {
                    objectList.add(resultSet.getString(sourceColumn));
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
                    Instant instant = resultSet.getTimestamp(sourceColumn).toInstant();
                    objectList.add(instant);
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
                    objectList.add(resultSet.getObject(sourceColumn));
                    break;
                }
                default:
                    break;
            }
        }
        return new AbstractMap.SimpleEntry<>(tokenRange, objectList.toArray());
    }

    @Override
    public void closeStorage() {
        cqlSession.close();
    }

    private int getBatchSize(ConnectionProperty connectionProperty) {
        String batchSize = connectionProperty.getToProperty().getProperty("batchSize");
        return  batchSize == null ? 100 : Integer.parseInt(batchSize);
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

/*
    public List<String> listOfTargetColumns(Chunk<?> chunk) throws SQLException {
        Config config = chunk.getConfig();
        java.sql.ResultSet resultSet = chunk.getResultSet();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            System.out.println(resultSetMetaData.getColumnName(i));
        }
        List<String> targetColumns = new ArrayList<>(config.columnToColumn() == null
                ? Collections.emptyList() : config.columnToColumn().values());
        targetColumns.addAll(config.expressionToColumn() == null ?
                Collections.emptyList() : config.expressionToColumn().values());
        return targetColumns;
    }
*/

    public Map<String, CassandraColumn> readTargetColumnsAndTypes(Chunk<?> chunk) {
        Map<String, CassandraColumn> columnMap = new HashMap<>();
        Config config = chunk.getConfig();
        KeyspaceMetadata keyspaceMetadata = metadata
                .getKeyspace(config.toSchemaName())
                .orElseThrow(() -> {
                    System.out.println("Here... " + config.toSchemaName());
                    return new RuntimeException();
                });
        Map<CqlIdentifier, ColumnMetadata> mapColumnMetaData = keyspaceMetadata
                .getTable(config.toTableName())
                .get()
                .getColumns();
//        mapColumnMetaData.forEach((c, v) -> System.out.println(v.getName() + " " + v.getType().toString().toLowerCase()));
        List<ColumnMetadata> columnMetadata = mapColumnMetaData.values().stream().toList();

        Map<String, String> columnToColumnMap = chunk.getConfig().columnToColumn();
        Map<String, String> expressionToColumnMap = chunk.getConfig().expressionToColumn();

        for (ColumnMetadata c : columnMetadata) {
            columnToColumnMap.entrySet()
                    .stream()
                    .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(c.getName().toString()))
                    .forEach(v -> columnMap.put(v.getKey(), new CassandraColumn(0, v.getValue(), c.getType().toString().toLowerCase())));
            if (chunk.getConfig().expressionToColumn() != null) {
                expressionToColumnMap.entrySet()
                        .stream()
                        .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(c.getName().toString()))
                        .forEach(v -> columnMap.put(c.getName().toString(), new CassandraColumn(0, c.getName().toString(), c.getType().toString().toLowerCase())));
            }
        }
        return columnMap;
    }
}
