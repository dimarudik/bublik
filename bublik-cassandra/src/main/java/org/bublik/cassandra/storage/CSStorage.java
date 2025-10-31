package org.bublik.cassandra.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import org.bublik.cassandra.model.CSChunk;
import org.bublik.cassandra.model.CSTable;
import org.bublik.cassandra.service.CSTableService;
import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.constants.PGKeywords;
import org.bublik.core.model.*;
import org.bublik.core.service.Source;
import org.bublik.core.storage.Storage;
import org.bublik.core.storage.StorageClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.bublik.cassandra.constants.SQLConstants.*;
import static org.bublik.core.util.Utils.getStackTrace;

public abstract class CSStorage<K extends UUID, T extends Long, S extends CqlSession, R extends ResultSet> extends Storage<K, T, S, R> implements Source {
    private static final Logger log = LoggerFactory.getLogger(CSStorage.class);
//    private final int batchSize;
    private final CSPool csPool;
    protected final int threadCount;
    private final ConnectionProperty connectionProperty;
    protected CSStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        this.connectionProperty = connectionProperty;
        this.threadCount = connectionProperty.getThreadCount();
//        this.batchSize = getBatchSize(connectionProperty);
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
    public S getPoolConnection() throws SQLException {
        return (S) csPool.getCqlSession();
    }

    @Override
    public void start(List<Config> cfgs, boolean sync, int rows, Storage<K, T, S, R> targetStorage, String tableName) throws SQLException {
        List<Config> configs = copyConfigs(cfgs);
        if (rows > 0) {
            createChunkTable(sync, tableName);
            createChunks(configs, sync, rows, tableName);
        }

        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        do {
            List<Chunk<K, T, S, R>> chunks = getChunkList(configs, tableName);
            List<Future<Chunk<?, ?, ?, ?>>> futures = new ArrayList<>();
            String tName = getChunkTableName(tableName);

            chunks.forEach(chunk -> futures.add(
                            service.submit(() -> {
                                chunk.setTargetStorage(targetStorage);
                                try {
                                    return chunk.allStages(false, tName);
                                } catch (Exception e) {
                                    log.error("ChunkId = {} {}.{} {}", chunk.getId(), chunk.getSourceTable().getSchemaName(), chunk.getSourceTable().getTableName(), getStackTrace(e));
                                    chunk.interStageSaveChunkStatus(ChunkStatus.PROCESSED_WITH_ERROR, sync, null, getStackTrace(e), tName);
                                    throw e;
                                }
                            })
                    )
            );

            int timeoutCounter = 0;
            for (Future<?> future : futures) {
                try {
                    Chunk<?, ?, ?, ?> c = (Chunk<?, ?, ?, ?>) future.get();
                    Thread.sleep(2);
                } catch (Exception e) {
                    if ((e.getMessage().contains("Query timed out after PT") ||
                            e.getMessage().contains("Cassandra timeout during BATCH"))
                            && timeoutCounter / threadCount < 20) {
                        try {
                            Thread.sleep(1_000);
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                        timeoutCounter++;
                        log.error("{}", getStackTrace(e));
                    } else {
                        log.error("{}", getStackTrace(e));
                        service.shutdownNow();
                        throw new RuntimeException(e);
                    }
                }
            }

            if (chunks.isEmpty()) {
                log.info("All chunks are processed");
                break;
            }
        } while (true);

//        dropChunkTable(sync, tableName);
        service.shutdown();
        service.close();
    }


    @Override
    public void createChunks(List<Config> configs, boolean sync, int rows, String tableName) throws SQLException {
        CqlSession cqlSession = csPool.getCqlSession();
        Set<TokenRange> trs = csPool.getTokenRanges();

        for (Config c : configs) {
            Table<?> pseudoTable = new PseudoTable<>(c.fromSchemaName(), c.fromTableName());
            List<Column> partitionKey = CSTableService.getKey(cqlSession, pseudoTable, "partition_key");
            List<Column> clusteringKey = CSTableService.getKey(cqlSession, pseudoTable, "clustering");
            CSTable<?> sourceTable = new CSTable<>(pseudoTable.getSchemaName(), pseudoTable.getTableName(), partitionKey, clusteringKey);
            trs.forEach(tr -> {
                long startValue = ((Murmur3Token) tr.getStart()).getValue();
                long stopValue = ((Murmur3Token) tr.getEnd()).getValue();
                if (stopValue > startValue) {
                    double estimatedRowsInRange = (long) getEstimatedRowsInRange(cqlSession, sourceTable, tr);
                    if (estimatedRowsInRange == 0) {
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

    private void insertChunk(CqlSession cqlSession, PreparedStatement ps, long start, long stop, CSTable<?> sourceTable, String taskName) {
        UUID chunkId = Uuids.timeBased();
        BoundStatement bs = ps.bind(
                chunkId,
                start,
                stop,
                sourceTable.getSchemaName(),
                sourceTable.getTableName(),
                "UNASSIGNED",
                taskName);
        cqlSession.execute(bs);
    }

    private double getEstimatedRowsInRange(CqlSession cqlSession, CSTable<?> sourceTable, TokenRange tr) {
        String queryCount = CSTableService.countRowsInTableQuery(sourceTable);
        PreparedStatement ps = cqlSession.prepare(queryCount);
        long startValue = ((Murmur3Token) tr.getStart()).getValue();
        long stopValue = ((Murmur3Token) tr.getEnd()).getValue();
        long delta = (stopValue - startValue) / 10;
        long g = delta / 10;
        long subRange = 0;
        long rowsInSubRange = 0;
        for (long j = startValue; j < startValue + delta; j = j + g ) {
            BoundStatement bsCount = ps.bind(j, j + g).setPageSize(10_000);
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
        try {
            CqlSession cqlSession = csPool.getCqlSession();
            cqlSession.execute(DDL_DROP_TABLE.replace("$tableName", getOutboxTableName(tableName)));
        } catch (Exception e) {
            log.info("Outbox table {} not found, nothing to drop", getOutboxTableName(tableName));
        }
    }

    private void createChunkTable(boolean sync, String tableName) {
        CqlSession cqlSession = csPool.getCqlSession();
        cqlSession.execute(DDL_CREATE_CHUNK_TABLE.replace("$tableName", getChunkTableName(tableName)));
    }

    @Override
    public String buildStartEndOfChunk(List<Config> configs, String chunkTableName) {
        List<String> taskNames = new ArrayList<>();
        configs.forEach(sqlStatement -> taskNames.add(sqlStatement.fromTaskName()));
        return "select chunk_id, start_page, end_page, task_name, schema_name, table_name, status from " +
                chunkTableName + " where task_name in ('" +
                String.join("', '", taskNames) +
                "') and status in ('ASSIGNED', 'UNASSIGNED', 'PROCESSED_WITH_ERROR') " +
                " and schema_name = ? and table_name = ? " +
                " per partition limit 1000 allow filtering ";
    }

    @Override
    public List<Chunk<K, T, S, R>> getChunkList(List<Config> configs, String chunkTableName) throws SQLException {
        List<Chunk<K, T, S, R>> chunks = new ArrayList<>();
        String sql = buildStartEndOfChunk(configs, getChunkTableName(chunkTableName));
        log.debug("SQL to fetch metadata of chunks: \n{}", sql);
        CqlSession cqlSession = csPool.getCqlSession();
        configs.forEach(c -> {
            Table<?> pseudoTable = new PseudoTable<>(c.fromSchemaName(), c.fromTableName());
            List<Column> partitionKey = CSTableService.getKey(cqlSession, pseudoTable, "partition_key");
            List<Column> clusteringKey = CSTableService.getKey(cqlSession, pseudoTable, "clustering");
            CSTable<?> sourceTable = new CSTable<>(pseudoTable.getSchemaName(), pseudoTable.getTableName(), partitionKey, clusteringKey);
            List<Column> columns = sourceTable.getAllColumns(cqlSession);
            sourceTable.setColumns(columns);
            PreparedStatement ps = cqlSession.prepare(sql);
            BoundStatement bs = ps.bind(sourceTable.getSchemaName(), sourceTable.getTableName());
            com.datastax.oss.driver.api.core.cql.ResultSet rs = cqlSession.execute(bs);
            for (Row row : rs) {
                String status = row.getString("status");
                chunks.add(
                        new CSChunk<>(
                                (K)row.getUuid("chunk_id"),
                                (T)(Long)row.getLong("start_page"),
                                (T)(Long)row.getLong("end_page"),
                                c,
                                sourceTable,
                                ChunkStatus.valueOf(status),
                                null,
                                this
                        ));
            }
        });
        return chunks;
    }

    @Override
    public void closeStorage() {
        csPool.closeCqlSession();
        log.info("Cassandra target storage stopped");
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
    public Table<S> configToTable(String schemaName, String tableName) {
        return null;
    }

    @Override
    public Table<S> getTagetTableBySourceTable(Table<S> table) {
        return null;
    }

    @Override
    public Table<S> getSourceTableByTargetTable(Table<S> table) {
        return null;
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
    public void close() throws Exception {
        log.info("Closing Cassandra storage");
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

    @Override
    public String buildFetchStatement(Config config, Table<?> sourceTable) {
        List<Column> pkColumns = new ArrayList<>(((CSTable<?>) sourceTable).getPartitionKey());
        Collections.sort(pkColumns);
        String pkColumnsJoined = String.join(", ", pkColumns.stream().map(Column::columnName).toList());
        List<String> strings = new ArrayList<>();
        Map<String, String> columnToColumnMap = config.columnToColumn();
        if (columnToColumnMap == null) {
            strings.addAll(
                    sourceTable.getColumns()
                            .stream()
                            .map(Column::columnName)
                            .toList()
            );
        } else {
            strings.addAll(columnToColumnMap.keySet());
        }
        Map<String, String> expressionToColumnMap = config.expressionToColumn();
        if (expressionToColumnMap != null) {
            strings.addAll(expressionToColumnMap.keySet());
        }
        String columnToColumn = String.join(", ", strings);
        return PGKeywords.SELECT + " " +
                columnToColumn + " " +
                PGKeywords.FROM + " " +
                config.fromSchemaName() +
                "." +
                config.fromTableName() + " " +
                (config.fromTableAdds() == null ? "" : config.fromTableAdds()) + " " +
                PGKeywords.WHERE + " " +
                "token(" +
                pkColumnsJoined +
                ") >= ? and token(" +
                pkColumnsJoined +
                ") < ?";
    }

    @Override
    public String buildFetchStatement(Config config) {
        return buildFetchStatement(config, null);
    }
}
