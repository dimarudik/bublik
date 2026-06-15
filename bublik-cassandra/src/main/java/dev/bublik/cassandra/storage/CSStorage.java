package dev.bublik.cassandra.storage;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;
import dev.bublik.core.model.*;
import dev.bublik.cassandra.model.CSChunk;
import dev.bublik.cassandra.model.CSTable;
import dev.bublik.cassandra.service.CSTableService;
import dev.bublik.cassandra.storage.cassandraaddons.MM3;
import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.constants.PGKeywords;
import dev.bublik.core.service.Source;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static dev.bublik.cassandra.constants.SQLConstants.*;
import static dev.bublik.core.constants.Constants.DEFAULT_FETCH_WHERE_CLAUSE;
import static dev.bublik.core.util.Utils.getStackTrace;

public abstract class CSStorage<K extends UUID, T extends Long, S extends CqlSession, R extends ResultSet> extends Storage<K, T, S, R> implements Source {
    private static final Logger log = LoggerFactory.getLogger(CSStorage.class);
    private final CSPool csPool;
    protected final int threadCount;
    private final ConnectionProperty connectionProperty;

    protected CSStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        this.connectionProperty = connectionProperty;
        this.threadCount = connectionProperty.getThreadCount();
        this.csPool = new CSPool(getStorageClass().getProperties(), connectionProperty.getThreadCount());
    }

    @Override
    public String getStorageVersion() {
        return "";
    }

    @Override
    public int getStorageMajorVersion() {
        return csPool.getMajorVersion();
    }

    @Override
    public S getSession() {
        return (S) getCsPool().getCqlSession();
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
    public void start(List<Config> cfgs, boolean sync, int rows, Storage<K, T, S, R> targetStorage, String tableName,  Storage<K, T, S, R> cacheStorage) throws SQLException {
        List<Config> configs = copyConfigs(cfgs);
        if (rows > 0) {
            dropChunkTable(configs, sync, tableName);
            createChunkTable(sync, tableName);
            fulfillChunks(configs, sync, rows, tableName);
            if (targetStorage instanceof JDBCStorage<?, ?, ?, ?>) {
                targetStorage.createGlobalOutbox(tableName);
            }
        }

        log.info("SOURCE Cassandra version: {}", getStorageMajorVersion());
//        log.info("TARGET Cassandra version: {}", targetStorage.getStorageMajorVersion());

        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        do {
            List<Chunk<K, T, S, R>> chunks = getChunkList(configs, tableName, targetStorage);
            List<Future<Chunk<?, ?, ?, ?>>> futures = new ArrayList<>();
            String tName = getChunkTableName(tableName);

            chunks.forEach(chunk -> futures.add(
                    service.submit(() -> {
                        try {
//                            log.info("chunk: {}", chunk.getId());
                            return chunk.allStages(false, tName);
                        } catch (Exception e) {
                            log.error("ChunkId = {} {}.{} {}", chunk.getId(), chunk.getT2t().sourceTable().getSchemaName(), chunk.getT2t().sourceTable().getTableName(), getStackTrace(e));
                            chunk.interStageSaveChunkStatus(ChunkStatus.PROCESSED_WITH_ERROR, sync, null, getStackTrace(e), tName);
                            throw e;
                        }
                    }))
            );

            int timeoutCounter = 0;
            for (Future<?> future : futures) {
                try {
                    Chunk<?, ?, ?, ?> c = (Chunk<?, ?, ?, ?>) future.get();
                    Thread.sleep(2);
                } catch (Exception e) {
                    if ((e.getMessage().contains("Query timed out after PT") ||
                            e.getMessage().contains("Cassandra timeout during BATCH") ||
                            e.getMessage().contains("failure during write query at consistency"))
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
        dropChunkTable(configs, sync, tableName);
//        targetStorage.dropOutboxTable(false, tableName);

        service.shutdown();
        service.close();
    }


    @Override
    public void fulfillChunks(List<Config> configs, boolean sync, int rows, String tableName) throws SQLException {
        S cqlSession = (S) csPool.getCqlSession();
        Set<TokenRange> trs = new HashSet<>(csPool.getTokenRanges());
        // добавляются хвостики сверху и снизу ренджа
        TokenRange defaultToken = MM3.defaultTokenRange();
        TokenRange firstToken = trs.stream().min(Comparator.comparing(TokenRange::getStart)).orElse(defaultToken);
        TokenRange lastToken = trs.stream().max(Comparator.comparing(TokenRange::getEnd)).orElse(defaultToken);
        Murmur3TokenFactory factory = new Murmur3TokenFactory();
        TokenRange floor = factory.range(defaultToken.getStart(), firstToken.getStart());
        TokenRange ceil = factory.range(lastToken.getEnd(), defaultToken.getEnd());
        trs.add(floor);
        trs.add(ceil);
//        log.info("floor: {} ceil: {}", floor, ceil);
        log.info("Token range size: {}", trs.size());
//        trs.forEach(t -> log.info("{} {}", t.getStart(), t.getEnd()));

        for (Config c : configs) {
            CSTable<S> sourceTable = new CSTable<>(c.fromSchemaName(), c.fromTableName(), null, null);
//            Table<S> targetTable = new PseudoTable<>(c.toSchemaName(), c.toTableName());
            sourceTable.enrichTable(cqlSession);
            ExecutorService service = Executors.newFixedThreadPool(Math.min(threadCount, 4));
            trs
                    .stream()
                    .filter(tr -> ((Murmur3Token) tr.getEnd()).getValue() > ((Murmur3Token) tr.getStart()).getValue())
                    .forEach(tr -> service.execute(() -> {
                        long startValue = ((Murmur3Token) tr.getStart()).getValue();
                        long stopValue = ((Murmur3Token) tr.getEnd()).getValue();
                        long estimatedRowsInRange = getEstimatedRowsInRange(cqlSession, sourceTable, tr);
                        log.info("Token range: {} {} estimatedRowsInRange: {}", startValue, stopValue, estimatedRowsInRange);
                        PreparedStatement ps = cqlSession.prepare(DML_INSERT_CHUNK_TABLE.replace("$tableName", getChunkTableName(tableName)));
                        if (estimatedRowsInRange <= rows) {
                            log.info("(estimatedRowsInRange <= rows) start:{} stop:{} estimated:{}", startValue, stopValue, estimatedRowsInRange);
                            insertChunk(cqlSession, ps, startValue, stopValue, sourceTable, c.fromTaskName(), estimatedRowsInRange);
                            return;
                        }
                        long chunkCount = (long) Math.ceil((double) estimatedRowsInRange / rows);
                        long shift = (stopValue - startValue) / chunkCount;
                        long i = startValue;
                        while ((i + shift) < stopValue) {
                            // проверка на переход по кругу long
                            if ((i + shift) < 0 && i > 0) {
                                break;
                            }
//                            log.info("start:{} stop:{} start+shift:{} shift:{} chunkCount:{} estimated:{}", i, stopValue, i + shift, shift, chunkCount, estimatedRowsInRange);
                            insertChunk(cqlSession, ps, i, i + shift, sourceTable, c.fromTaskName(), rows);
                            i += shift;
                        }
                        if (i < stopValue && i > startValue) {
//                            log.info("last chunk start:{} stop:{} estimated:{}", startValue, stopValue, estimatedRowsInRange);
                            insertChunk(cqlSession, ps, i, stopValue, sourceTable, c.fromTaskName(), rows);
                        }
                    }));
            service.shutdown();
            service.close();
        }
        log.info("Chunk table fulfilled successfully");
    }

    private long getEstimatedRowsInRange(CqlSession cqlSession, CSTable<?> sourceTable, TokenRange tr) {
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
            ResultSet rs = cqlSession.execute(bsCount);
            long rCount = 0;
            for (Row row : rs) {
                rCount++;
            }
            subRange += g;
            rowsInSubRange += rCount;
        }
        if (subRange == 0 || rowsInSubRange == 0)
            return 0;
        return (stopValue - startValue) / subRange * rowsInSubRange;
    }

    private void insertChunk(CqlSession cqlSession, PreparedStatement ps, long start,
                             long stop, CSTable<?> sourceTable, String taskName, long shift) {
        try {
            UUID chunkId = Uuids.timeBased();
            BoundStatement bsInsert = ps.boundStatementBuilder()
                    .setUuid("chunk_id", chunkId)
                    .setLong("start_page", start)
                    .setLong("end_page", stop)
                    .setString("schema_name", sourceTable.getSchemaName())
                    .setString("table_name", sourceTable.getTableName())
                    .setString("status", "UNASSIGNED")
                    .setString("task_name", taskName)
                    .setInt("required", (int) shift )
                    .setString("thread", Thread.currentThread().getName())
                    .build();
            cqlSession.execute(bsInsert);
        } catch (Exception e) {
            log.error("{}", getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropChunkTable(List<Config> configs, boolean sync, String tableName) throws SQLException {
        CqlSession cqlSession = csPool.getCqlSession();
        try {
            cqlSession.execute(DDL_DROP_TABLE.replace("$tableName", getChunkTableName(tableName)));
        } catch (Exception e) {
            log.warn("Table {} not found", getChunkTableName(tableName));
        }
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

    public String getOutboxTableName(String tName) {
        String tableName = tName.replace("\"", "");
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
    public void createGlobalOutbox(String tableName) throws SQLException {
        CqlSession cqlSession = csPool.getCqlSession();
        cqlSession.execute(DDL_CREATE_GLOBAL_OUTBOX_TABLE.replace("$tableName", getOutboxTableName(tableName)));
        log.info("Global outbox table created successfully");
    }

    @Override
    public boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk, String tableName) {
        CqlSession cqlSession = (CqlSession) chunk.getTargetSession();
        String selectCQL = DML_SELECT_OUTBOX_TABLE.replace("$tableName", getOutboxTableName(tableName));
        com.datastax.oss.driver.api.core.cql.ResultSet rs = cqlSession.execute(selectCQL, chunk.getId());
        return rs.one() != null;
    }

    @Override
    public void insertProcessedChunkInfo(Chunk<?, ?, ?, ?> chunk, String tableName) {
        CqlSession cqlSession = (CqlSession) chunk.getTargetSession();
        String insertCQL = DML_INSERT_OUTBOX_TABLE.replace("$tableName", getOutboxTableName(tableName));
        PreparedStatement statement = cqlSession.prepare(insertCQL);
        BoundStatement boundStatement = statement.bind(chunk.getId(), chunk.getConfig().fromTaskName(), chunk.getCopied())
                .setPageSize(1_000)
                .setTimeout(Duration.ofSeconds(1))
                .setConsistencyLevel(ConsistencyLevel.QUORUM);
        cqlSession.execute(boundStatement);
//        cqlSession.execute(insertCQL, chunk.getId(), chunk.getConfig().fromTaskName(), chunk.getCopied());
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
    public String buildStartEndOfChunk(Config config, String chunkTableName, Table<S> sourceTable) {
        return "select chunk_id, start_page, end_page, task_name, schema_name, table_name, status from " +
                chunkTableName + " where " +
                "status in ('ASSIGNED', 'UNASSIGNED', 'PROCESSED_WITH_ERROR') " +
                " and schema_name = ? and table_name = ? " +
                " per partition limit 1000 ";
    }

    @Override
    public List<Chunk<K, T, S, R>> getChunkList(List<Config> configs, String chunkTableName, Storage<K, T, S, R> targetStorage) throws SQLException {
        List<Chunk<K, T, S, R>> chunks = new ArrayList<>();
        CqlSession sourceSession = getSession();
//        log.info("Get chunk list from {}", chunkTableName);
        configs.forEach(config -> {
            Table<S> sourceTable = this.configToTable(config.fromSchemaName(), config.fromTableName());
            Table<S> targetTable = targetStorage.configToTable(config.toSchemaName(), config.toTableName());
            try {
                this.enrichTable(sourceTable);
                targetStorage.enrichTable(sourceTable, targetTable);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
//            targetTable.getColumns().forEach(c -> System.out.println(c.columnName() + "." + c.columnType()));
            String orderByClause = targetTable.buildOrderBy(config);
            List<Column2Column> c2c = getColumn2Column(sourceTable, targetTable, config);
            Table2Table<S> t2t = getTable2Table(sourceTable, targetTable, c2c, config);
            String sql = buildStartEndOfChunk(config, getChunkTableName(chunkTableName), sourceTable);
            log.debug("Query of chunks for table {}.{}: {}", t2t.sourceTable().getSchemaName(), t2t.sourceTable().getTableName(), sql);
            String fetchQuery = buildFetchStatement(config, t2t);
            log.info("Fetch query: {}", fetchQuery);
            PreparedStatement ps = sourceSession.prepare(sql);
            BoundStatement bs = ps.bind(sourceTable.getSchemaName(), sourceTable.getTableName()).setConsistencyLevel(ConsistencyLevel.QUORUM);
            ResultSet rs = sourceSession.execute(bs);
            for (Row row : rs) {
                String status = row.getString("status");
                String taskName = row.getString("task_name");
                assert taskName != null;
                if (taskName.equals(config.fromTaskName())) {
                    Chunk<K, T, S, R> chunk =
                            new CSChunk<>(
                                    (K) row.getUuid("chunk_id"),
                                    (T) (Long) row.getLong("start_page"),
                                    (T) (Long) row.getLong("end_page"),
                                    config,
                                    t2t,
                                    ChunkStatus.valueOf(status),
                                    fetchQuery,
                                    this,
                                    targetStorage,
                                    orderByClause);
                    chunks.add(chunk);
                }
            }
        });
        return chunks;
    }

    @Override
    public Table2Table<S> getTable2Table(Table<S> sourceTable,
                                          Table<S> targetTable,
                                          List<Column2Column> c2c,
                                          Config config) {
        Column ttlColumn = null;
        Column timestampColumn = null;
        if (config.withTTL() != null) {
            ttlColumn = new Column(-1,
                    "_ttl",
                    "int",
                    config.withTTL());
        }
        if (config.timestamp() != null) {
            timestampColumn = new Column(-1,
                    "_timestamp",
                    "int",
                    config.timestamp());
        }
        return new Table2Table<>(sourceTable, targetTable, c2c, ttlColumn, timestampColumn);
    }

    @Override
    public List<Column2Column> getColumn2Column(Table<S> sourceTable, Table<S> targetTable, Config config) {
        List<Column2Column> column2Column = new ArrayList<>();
        if (config.columnToColumn() == null && config.expressionToColumn() == null) {
            if (sourceTable.getClass() == targetTable.getClass()) {
                sourceTable.getColumns().forEach(c -> column2Column.add(new Column2Column(c, c)));
            } else {
                targetTable.getColumns().forEach(c -> column2Column.add(new Column2Column(c,
                        targetTable
                                .getColumns()
                                .stream()
                                .filter(c1 -> c1.columnName().equals(c.columnName())).findFirst().orElseThrow())));
            }
        }
        if (config.columnToColumn() != null) {
            for (Map.Entry<String,String> entry : config.columnToColumn().entrySet()) {
                Column sourceColumn = sourceTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes().equals(entry.getKey().replaceAll("\"", "")))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(entry.getKey() + " not found in source table " +
                                sourceTable.getSchemaName() + "." + sourceTable.getTableName()));
                Column targetColumn = targetTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes().equals(entry.getValue().replace("\"", "")))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(entry.getValue() + " not found in target table " +
                                targetTable.getSchemaName() + "." + targetTable.getTableName()));
                column2Column.add(new Column2Column(sourceColumn, targetColumn));
            }
        }
        if (config.expressionToColumn() != null) {
            for (Map.Entry<String,String> entry : config.expressionToColumn().entrySet()) {
                Column column = targetTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes().equals(entry.getValue().replace("\"", "")))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(entry.getValue() + " not found in target table " +
                                targetTable.getSchemaName() + "." + targetTable.getTableName()));
                column2Column.add(new Column2Column(column, column, entry.getKey()));
            }
        }
//        logColumn2Column(column2Column);
        return column2Column;
    }

/*
    private void logColumn2Column(List<Column2Column> column2Column) {
        column2Column.forEach(c2c -> log.info("Column2Column: {} {} {} -> {} {}",
                c2c.sourceExpression(),
                c2c.sourceColumn().columnName(), c2c.sourceColumn().columnType(),
                c2c.targetColumn().columnName(), c2c.targetColumn().columnType()));
    }
*/

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
        return new CSTable<>(schemaName, tableName, null, null);
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
    public String buildFetchStatement(Config config, Table2Table<S> t2t) {
        CSTable<?> sourceTable = (CSTable<?>) t2t.sourceTable();
        List<Column> pkColumns = new ArrayList<>(sourceTable.getPartitionKey());
        List<Column> ckColumns = new ArrayList<>(sourceTable.getClusteringKey());
        List<Column> nonStaticColumns = CSTableService.getNonStaticColumns(sourceTable);
        List<Column> nonFrozenCollectionColumns = getStorageMajorVersion() < 5
                ? CSTableService.getNonFrozenCollectionColumns(sourceTable) : new ArrayList<>();
//        System.out.println("nonFrozenCollectionColumns: " + nonFrozenCollectionColumns);
        Collections.sort(pkColumns);
        String pkColumnsJoined = String.join(", ", pkColumns.stream().map(Column::columnName).toList());
        List<String> columns = new ArrayList<>(t2t.column2Columns()
                .stream()
                .map(c2c -> c2c.sourceExpression() == null ? c2c.sourceColumn().columnName() : c2c.sourceExpression())
                .toList());
        List<String> ttlColumns = columns
                .stream()
                .filter(s -> pkColumns.stream().noneMatch(pk -> pk.columnName().equals(s)))
                .filter(s -> ckColumns.stream().noneMatch(ck -> ck.columnName().equals(s)))
                .filter(s -> nonStaticColumns.stream().anyMatch(ns -> ns.columnName().equals(s)))
                .filter(s -> nonFrozenCollectionColumns.stream().noneMatch(nfc -> nfc.columnName().equals(s)))
                .map(s -> "ttl(" + s + ")")
                .toList();
        List<String> timestampColumns = columns
                .stream()
                .filter(s -> pkColumns.stream().noneMatch(pk -> pk.columnName().equals(s)))
                .filter(s -> ckColumns.stream().noneMatch(ck -> ck.columnName().equals(s)))
                .filter(s -> nonStaticColumns.stream().anyMatch(ns -> ns.columnName().equals(s)))
                .filter(s -> nonFrozenCollectionColumns.stream().noneMatch(nfc -> nfc.columnName().equals(s)))
                .map(s -> "writetime(" + s + ")")
                .toList();
        columns.addAll(ttlColumns);
        columns.addAll(timestampColumns);
        String columnToColumn = String.join(", ", columns);
        return PGKeywords.SELECT + " " +
                columnToColumn + " " +
//                (t2t.ttlColumn() == null ? "" : ( t2t.ttlColumn().defaultValue().equals("NULL")  ?  (" , (int)NULL as  \""  + t2t.ttlColumn().columnName() + "\" ") : (", cast((int)0 + " + t2t.ttlColumn().defaultValue() + " as int ) as \"" + t2t.ttlColumn().columnName() + "\" ")  ) ) +
                (t2t.ttlColumn() == null ? "" : ( t2t.ttlColumn().defaultValue().equals("NULL")  ?  (" , (int)NULL as  \""  + t2t.ttlColumn().columnName() + "\" ") : (", " + t2t.ttlColumn().defaultValue() + " as \"" + t2t.ttlColumn().columnName() + "\" ")  ) ) +
                (t2t.timestampColumn() == null ? "" : ( ", " + t2t.timestampColumn().defaultValue() + " as \"" + t2t.timestampColumn().columnName() + "\" ")) +
                PGKeywords.FROM + " " +
                config.fromSchemaName() +
                "." +
                config.fromTableName() + " " +
                (config.fromTableAdds() == null ? "" : config.fromTableAdds()) + " " +
                PGKeywords.WHERE + " " +
                (config.fetchWhereClause().equals(DEFAULT_FETCH_WHERE_CLAUSE) ? "" : config.fetchWhereClause() + " and ") + " " +
                "token(" +
                pkColumnsJoined +
                ") >= ? and token(" +
                pkColumnsJoined +
                ") < ?" +
                (config.fetchWhereClause().equals(DEFAULT_FETCH_WHERE_CLAUSE) ? "" : " allow filtering ");
    }

    @Override
    public void setSession(S session) {
    }

    @Override
    public void enrichTable(Table<S> sourceTable) throws SQLException {
        sourceTable.enrichTable(getSession());
    }

    @Override
    public void enrichTable(Table<S> sourceTable, Table<S> targetTable) throws SQLException {
        targetTable.enrichTable(getSession());
    }

    @Override
    public <V, W> void insertColumnValue(List<ColumnValue<V>> columnValues, Chunk<K, T, S, R> chunk, W writer) {
    }

    @Override
    public <W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        return null;
    }

    @Override
    public <W> void closeWriter(W writer, Chunk<K, T, S, R> chunk, String tableName) throws SQLException {

    }
}
