package org.bublik.storage;

import com.datastax.driver.core.*;
import org.bublik.constants.PGKeywords;
import org.bublik.model.*;
import org.bublik.storage.cassandraaddons.LongTokenRange;
import org.bublik.storage.cassandraaddons.MM3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CassandraStorage extends Storage {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStorage.class);
    private final Cluster cluster;
    private final Session session;
    private final Metadata metadata;
    private final int batchSize;
    private final Set<TokenRange> tokenRangeSet;
//    private final Configuration configuration;

    public CassandraStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        this.cluster = Cluster
                .builder()
                .addContactPoint(getStorageClass().getProperties().getProperty("host"))
                .withPort(Integer.parseInt(getStorageClass().getProperties().getProperty("port")))
                .withQueryOptions(queryOptions)
                .withoutJMXReporting()
                .withCredentials(getStorageClass().getProperties().getProperty("user"), getStorageClass().getProperties().getProperty("password"))
                .build();
        this.metadata = cluster.getMetadata();
        this.tokenRangeSet = metadata.getTokenRanges();
        this.session = cluster.connect();
        this.batchSize = getBatchSize(connectionProperty);
//        this.configuration = cluster.getConfiguration();
    }

    @Override
    public void start(List<Config> configs) throws SQLException {

    }

    @Override
    public boolean hook(List<Config> configs) throws SQLException {
        return false;
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
        return simpleBatch(chunk);
//        return simpleInsert(chunk);
//        return rangedBatch(chunk);
    }

    public LogMessage simpleInsert(Chunk<?> chunk) throws SQLException {
        int recordCount = 0;
        long start = System.currentTimeMillis();
        Map<String, CassandraColumn> stringCassandraColumnMap = readTargetColumnsAndTypes(chunk);
        String insertString = buildInsertStatement(chunk, stringCassandraColumnMap);
        PreparedStatement preparedStatement = session.prepare(insertString);
        java.sql.ResultSet resultSet = chunk.getResultSet();
        while (resultSet.next()) {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            Object[] objects = getBoundStatement(resultSet, stringCassandraColumnMap);
            boundStatement.bind(objects);
            session.execute(boundStatement);
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
//        LOGGER.info(insertString);
//        stringCassandraColumnMap
//                .forEach((k, v) -> System.out.println(k + " " + v.getColumnPosition() + "." + v.getColumnName() + "." + v.getColumnType()));
        BatchStatement batchStatement = new BatchStatement();
        PreparedStatement preparedStatement = session.prepare(insertString);
        java.sql.ResultSet resultSet = chunk.getResultSet();
        while (resultSet.next()) {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            Object[] objects = getBoundStatement(resultSet, stringCassandraColumnMap);
            boundStatement.bind(objects);
            batchStatement.add(boundStatement);
            recordCount++;
            // batch_size_fail_threshold_in_kb: 50
            if (recordCount % batchSize == 0) {
                session.execute(batchStatement);
                batchStatement.clear();
            }
        }
        session.execute(batchStatement);
        long stop = System.currentTimeMillis();
        return new LogMessage(
                recordCount,
                start,
                stop,
                "Cassandra SIMPLE BATCH APPLY",
                chunk);
    }

    public LogMessage rangedBatch(Chunk<?> chunk) throws SQLException {
        int threads = tokenRangeSet.size();
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        tokenRangeSet.forEach(tokenRange -> System.out.println(tokenRange.getStart() + " " + tokenRange.getEnd()));

//        MM3 mm3 = new MM3("1000");
//        System.out.println("key = 1000 between " + mm3.getLongTokenRange(tokenRangeSet).getStart() +
//                " and " + mm3.getLongTokenRange(tokenRangeSet).getEnd());

        int recordCount = 0;
        long start = System.currentTimeMillis();
//        LOGGER.info(insertString);
//        stringCassandraColumnMap
//                .forEach((k, v) -> System.out.println(k + " " + v.getColumnPosition() + "." + v.getColumnName() + "." + v.getColumnType()));
        try {
            Map<String, CassandraColumn> stringCassandraColumnMap = readTargetColumnsAndTypes(chunk);
            String insertString = buildInsertStatement(chunk, stringCassandraColumnMap);
            BatchStatement batchStatement = new BatchStatement();
            PreparedStatement preparedStatement = session.prepare(insertString);
            java.sql.ResultSet resultSet = chunk.getResultSet();
            while (resultSet.next()) {
                BoundStatement boundStatement = new BoundStatement(preparedStatement);
                Object[] objects = getBoundStatement(resultSet, stringCassandraColumnMap);
                boundStatement.bind(objects);
                batchStatement.add(boundStatement);
                recordCount++;
                if (recordCount % batchSize == 0) {
                    session.execute(batchStatement);
                    batchStatement.clear();
                }
            }
            session.execute(batchStatement);
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
                "Cassandra RANGED BATCH APPLY",
                chunk);
    }

    private Object[] getBoundStatement(java.sql.ResultSet resultSet,
                                       Map<String, CassandraColumn> stringCassandraColumnMap) throws SQLException {
        // http://www.java2s.com/example/java-api/org/apache/cassandra/dht/murmur3partitioner/murmur3partitioner-0-0.html
        List<Object> objectList = new ArrayList<>();
        for (Map.Entry<String, CassandraColumn> entry : stringCassandraColumnMap.entrySet()) {
            String sourceColumn = entry.getKey().replaceAll("\"", "");
            String targetType = entry.getValue().getColumnType();
            switch (targetType) {
                case "int": {
                    int v = resultSet.getInt(sourceColumn);
/*
                    if (entry.getValue().getColumnName().equals("id")) {
                        MM3 mm3 = new MM3(String.valueOf(v));
                        System.out.println(v + " : " + mm3.getTokenLong());
                        try {
                            LongTokenRange longTokenRange = mm3.getLongTokenRange(tokenRangeSet);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
*/
                    objectList.add(v);
//                    objectList.add(resultSet.getInt(sourceColumn));
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
                    LocalDate localDate = LocalDate.fromMillisSinceEpoch(resultSet.getDate(sourceColumn).getTime());
                    objectList.add(localDate);
                    break;
                }
                case "timestamp": {
                    objectList.add(resultSet.getTimestamp(sourceColumn));
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
        return objectList.toArray();
    }

    @Override
    public void closeStorage() {
        session.close();
        cluster.close();
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
        List<ColumnMetadata> columnMetadata = metadata.getKeyspace(config.toSchemaName()).getTable(config.toTableName()).getColumns();
        Map<String, String> columnToColumnMap = chunk.getConfig().columnToColumn();
        Map<String, String> expressionToColumnMap = chunk.getConfig().expressionToColumn();

        for (ColumnMetadata c : columnMetadata) {
            columnToColumnMap.entrySet()
                    .stream()
                    .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(c.getName()))
                    .forEach(v -> columnMap.put(v.getKey(), new CassandraColumn(0, v.getValue(), c.getType().getName().toString())));
            if (chunk.getConfig().expressionToColumn() != null) {
                expressionToColumnMap.entrySet()
                        .stream()
                        .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(c.getName()))
                        .forEach(v -> columnMap.put(c.getName(), new CassandraColumn(0, c.getName(), c.getType().getName().toString())));
            }
        }
        return columnMap;
    }
}
