package org.bublik.storage;

import com.datastax.driver.core.*;
import org.bublik.constants.PGKeywords;
import org.bublik.model.*;
import org.bublik.service.ChunkService;
import org.bublik.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CassandraStorage extends Storage implements StorageService {
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
                .build();
        this.metadata = cluster.getMetadata();
        this.tokenRangeSet = metadata.getTokenRanges();
        this.session = cluster.connect();
        this.batchSize = getBatchSize(connectionProperty);
//        this.configuration = cluster.getConfiguration();
    }

    public Cluster getCluster() {
        return cluster;
    }

    @Override
    public void startWorker(List<Future<LogMessage>> futures, List<Config> configs, ExecutorService executorService) throws SQLException {

    }

    @Override
    public LogMessage callWorker(Chunk<?> chunk, Map<String, Integer> columnsFromDB) throws SQLException {
        return null;
    }

    @Override
    public LogMessage transferToTarget(ResultSet resultSet) throws SQLException {
        ExecutorService executorService = Executors.newFixedThreadPool(tokenRangeSet.size());
        Chunk<?> chunk = ChunkService.get();
        int recordCount = 0;
        long start = System.currentTimeMillis();
        String insertString = buildInsertStatement(chunk);
        LOGGER.info(insertString);
        BatchStatement batchStatement = new BatchStatement();
//        PreparedStatement preparedStatement = session.prepare("insert into store.target (id, boolean) values (?, true)");
        PreparedStatement preparedStatement = session.prepare(insertString);
        while (resultSet.next()) {
            batchStatement.add(preparedStatement.bind(resultSet.getInt("ID")));
            recordCount++;
            // batch_size_fail_threshold_in_kb: 50
            if (recordCount % batchSize == 0) {
                session.execute(batchStatement);
                batchStatement.clear();
            }
        }
        session.execute(batchStatement);
        executorService.shutdown();
        executorService.close();
        return new LogMessage(
                recordCount,
                start,
                "Cassandra BATCH APPLY",
                chunk);
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

    public String buildInsertStatement(Chunk<?> chunk) {
        Config config = chunk.getConfig();
        List<String> targetColumns = new ArrayList<>(config.columnToColumn() == null
                ? Collections.emptyList() : config.columnToColumn().values());
        targetColumns.addAll(config.expressionToColumn() == null ?
                Collections.emptyList() : config.expressionToColumn().values());
        return PGKeywords.INSERT + " " + PGKeywords.INTO + " " +
                config.toSchemaName() + "." +
                config.toTableName() + " (" +
                String.join(", ", targetColumns) + ") " +
                PGKeywords.VALUES + " (" +
                targetColumns.stream().map(c -> "?").collect(Collectors.joining(",")) +
                ")";
    }

    public Map<String, PGColumn> readTargetColumnsAndTypes(Chunk<?> chunk) {
        Map<String, PGColumn> columnMap = new HashMap<>();
        Config config = chunk.getConfig();
        List<ColumnMetadata> columnMetadata = metadata.getKeyspace(config.toSchemaName()).getTable(config.toTableName()).getColumns();
        columnMetadata.forEach(c -> System.out.println(c.getType().getName()));
/*
        try {
            List<ColumnMetadata> columnMetadata = metadata.getKeyspace(config.toSchemaName()).getTable(config.toTableName()).getColumns();
            Map<String, String> columnToColumnMap = chunk.getConfig().columnToColumn();
            Map<String, String> expressionToColumnMap = chunk.getConfig().expressionToColumn();
            while (resultSet.next()) {
                String columnName = resultSet.getString(4);
                String columnType = resultSet.getString(6);
                Integer columnPosition = resultSet.getInt(17);

                columnToColumnMap.entrySet()
                        .stream()
                        .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(columnName))
                        .forEach(i -> columnMap.put(i.getKey(), new PGColumn(columnPosition, i.getValue(), columnType.equals("bigserial") ? "bigint" : columnType)));

                if (chunk.getConfig().expressionToColumn() != null) {
                    expressionToColumnMap.entrySet()
                            .stream()
                            .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(columnName))
                            .forEach(i -> columnMap.put(columnName, new PGColumn(columnPosition, i.getValue(), columnType.equals("bigserial") ? "bigint" : columnType)));
                }
            }
            resultSet.close();
        } catch (SQLException e) {
            System.out.println(e);
        }
*/
        return columnMap;
    }
}
