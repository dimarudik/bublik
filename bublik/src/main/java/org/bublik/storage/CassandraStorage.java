package org.bublik.storage;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import org.bublik.model.*;
import org.bublik.storage.cassandraaddons.BatchEntity;
import org.bublik.storage.cassandraaddons.CSObject;
import org.bublik.storage.cassandraaddons.MM3;
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

import static org.bublik.exception.Utils.getStackTrace;

public class CassandraStorage extends Storage {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStorage.class);
    private final int batchSize;
    private final CqlSession cqlSession;

    public CassandraStorage(StorageClass storageClass,
                            ConnectionProperty connectionProperty,
                            Boolean isSource) {
        super(storageClass, connectionProperty, isSource);
        Properties properties = getStorageClass().getProperties();
        DriverConfigLoader configLoader = DriverConfigLoader
                .programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(Long.parseLong(properties.getProperty("query_time_out"))))
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
/*
        this.tokenRangeSet = metadata.getTokenMap().orElseThrow().getTokenRanges();
        tokenRangeSet
                .forEach(tokenRange -> System.out.println(((Murmur3Token)tokenRange.getStart()).getValue() + " : " +
                        ((Murmur3Token)tokenRange.getEnd()).getValue()));
*/
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
//        LogMessage logMessage = simpleInsert(chunk);
//        return simpleBatch(chunk);
        LogMessage logMessage = rangedBatch(chunk);
//        LogMessage logMessage = new LogMessage(0, 0, 0, "", chunk);
        closeStorage();
        return logMessage;
    }

/*
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
*/

/*
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
*/

    public LogMessage rangedBatch(Chunk<?> chunk) throws SQLException {
        int recordCount = 0;
        long start = System.currentTimeMillis();
        try {
            CSObject csObject = new CSObject(cqlSession)
                    .metadata()
                    .tokenRangeSet()
                    .mm3batch()
                    .cassandraColumnMap(chunk)
                    .query(chunk)
                    .preparedStatement();

/*
            LOGGER.info("{} {} {} {} {}", csObject.getMetadata().getClusterName(),
                    csObject.getTokenRangeSet().size(),
                    csObject.getMm3Batch().getTokenRangeMap().size(),
                    csObject.getCassandraColumnMap().size(),
                    csObject.getQuery());
*/

            ResultSet resultSet = chunk.getResultSet();
            while (resultSet.next()) {
                Map.Entry<TokenRange, Object[]> entry = getPreparedStatementObjects(resultSet,
                        csObject.getCassandraColumnMap(), csObject.getTokenRangeSet());
                Map<TokenRange, BatchEntity> tokenRangeBatchEntityMap = csObject.getMm3Batch().getTokenRangeMap();
                BatchEntity batchEntity = tokenRangeBatchEntityMap.get(entry.getKey());
                BatchStatementBuilder batchStatementBuilder = batchEntity.getBatchStatementBuilder();
                batchStatementBuilder.addStatement(csObject.getPreparedStatement().bind(entry.getValue()));
                batchEntity.increaseCounter();
                recordCount++;
                // batch_size_fail_threshold_in_kb: 50
                if (batchEntity.getCounter() == batchSize) {
                    batchApply(batchStatementBuilder);
                    batchEntity.resetCounter();
                }
            }
            for (Map.Entry<TokenRange, BatchEntity> entry : csObject.getMm3Batch().getTokenRangeMap().entrySet()) {
                if (entry.getValue().getCounter() > 0) {
                    batchApply(entry.getValue().getBatchStatementBuilder());
                }
            }
        } catch (Exception e) {
            throw e;
        }
        long stop = System.currentTimeMillis();
        return new LogMessage(
                recordCount,
                start,
                stop,
                "Cassandra RANGED BATCH APPLY",
                chunk);
    }

    private void batchApply(BatchStatementBuilder batchStatementBuilder) {
        BatchStatement batchStatement = batchStatementBuilder
                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                .setTimeout(Duration.ofSeconds(60))
                .build();
        cqlSession
                .executeAsync(batchStatement)
                .whenComplete((asyncResultSet, throwable) -> {
                    if (throwable != null) {
                        LOGGER.info("{}", getStackTrace(throwable));
                    }
                });
        batchStatementBuilder.clearStatements();
        batchStatement.clear();
    }

    private Map.Entry<TokenRange, Object[]> getPreparedStatementObjects(ResultSet resultSet,
                                                                        Map<String, CassandraColumn> stringCassandraColumnMap,
                                                                        Set<TokenRange> tokenRangeSet) throws SQLException {
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

                    }
                    objectList.add(v);
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
}
