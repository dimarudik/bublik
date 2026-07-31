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
import dev.bublik.cassandra.model.CSChunk;
import dev.bublik.cassandra.model.CSTable;
import dev.bublik.cassandra.service.CSTableService;
import dev.bublik.cassandra.storage.cassandraaddons.MM3;
import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.constants.PGKeywords;
import dev.bublik.core.model.*;
import dev.bublik.core.service.Source;
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

import static dev.bublik.cassandra.constants.SQLConstants.*;
import static dev.bublik.core.constants.Constants.DEFAULT_FETCH_WHERE_CLAUSE;
import static dev.bublik.core.util.Utils.getStackTrace;

abstract class CSStorage extends Storage implements Source {
    private static final Logger log = LoggerFactory.getLogger(CSStorage.class);
    private CSPool csPool;
    protected int batchSize;
    private final String keySpace;

    protected CSStorage(StorageClass storageClass,
                        ConnectionProperty connectionProperty,
                        Table outboxTable) {
        super(storageClass, connectionProperty, outboxTable);
        this.batchSize = getBatchSize(connectionProperty);
        this.threadCount = connectionProperty.getThreadCount();
        this.csPool = new CSPool(getStorageClass().getProperties(), connectionProperty.getThreadCount());
        this.isManaged = true;
        this.keySpace = getStorageClass().getProperties().getProperty("keyspace");
        this.outboxTable = new DummyTable.Builder(keySpace, "bublik").build();
    }

    protected CSStorage(Builder<?, ?> builder) {
        super(builder);
        this.batchSize = builder.batchSize;
        this.keySpace = builder.keySpace;
        if (builder.cqlSession != null) {
            this.csPool = new CSPool(builder.cqlSession);
            if (threadCount <= 0) {
                this.threadCount = this.csPool.getSize();
            }
        }
        if (outboxTable == null) {
            this.outboxTable = new DummyTable.Builder(keySpace, "bublik").build();
        }
    }

    static abstract class Builder<C extends CSStorage, B extends Storage.Builder<C, B>>
            extends Storage.Builder<C, B> {
        private int batchSize;
        private final CqlSession cqlSession;
        private final String keySpace;

        protected Builder(CqlSession cqlSession, String keySpace) {
            this.cqlSession = cqlSession;
            this.keySpace = keySpace;
        }

        public B batchSize(int batchSize) {
            this.batchSize = batchSize;
            return self();
        }
    }

    @Override
    public void validate(Storage targetStorage, List<Config> configs) throws SQLException {

    }

    @Override
    public String getStorageVersion() {
        return csPool.getVersion();
    }

    @Override
    public int getStorageMajorVersion() {
        return csPool.getMajorVersion();
    }

    @Override
    public <S extends AutoCloseable> S getSession() {
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
    public <S extends AutoCloseable> S getPoolConnection() throws SQLException {
        return (S) csPool.getCqlSession();
    }

/*
    @Override
    public void start(Storage targetStorage, List<Config> cfgs, int rows) throws SQLException {
        List<Config> configs = copyConfigs(cfgs);
        if (rows > 0) {
            preChecks(configs);
            createChunkTable();
            fulfillChunks(configs, false, rows);
            if (targetStorage instanceof JDBCStorage) {
                targetStorage.createGlobalOutbox();
            }
        }
        log.info("SOURCE version: {}", getStorageVersion());
        log.info("TARGET version: {}", targetStorage.getStorageVersion());

        int errorCounter = 0;
        log.info("THREADS: {}", threadCount);
        log.info("FETCH_SIZE: {}", getFetchSize());
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        do {
            List<Chunk<?, ?, ?, ?>> chunks = getChunkList(configs, targetStorage);
            List<Future<Chunk<?, ?, ?, ?>>> futures = new ArrayList<>();

            chunks.forEach(chunk -> futures.add(
                    service.submit(() -> {
                        try {
                            return chunk.allStages(false, getOutboxTable());
                        } catch (Exception e) {
                            log.error("ChunkId = {} {}.{} {}", chunk.getId(), chunk.getT2t().sourceTable().getSchemaName(), chunk.getT2t().sourceTable().getTableName(), getStackTrace(e));
                            log.warn("Saving info about error to database");
                            chunk.interStageSaveChunkStatus(ChunkStatus.PROCESSED_WITH_ERROR, false, null, getStackTrace(e), getOutboxTable().tableToString());
                            if (targetStorage instanceof  JDBCStorage) {
                                (chunk.getTargetSession()).close();
                            }
                            throw new RuntimeException("ChunkId = " + chunk.getId() + " " + e.getMessage(), e);
                        }
                    }))
            );

            boolean hasBatchErrors = false;
            Throwable lastSubmittedException = null;

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    hasBatchErrors = true;
                    lastSubmittedException = e;
                    errorCounter++;
                }
            }

            if (hasBatchErrors) {
                if (errorCounter <= (threadCount * 2)) {
                    log.warn("Try: {} Continue...", errorCounter);
                    continue;
                } else {
                    log.error("Try: {} Unrecoverable error: {}", errorCounter, getStackTrace(lastSubmittedException));
                    log.info("Finishing due to critical stress failure...");
                    service.shutdownNow();
                    try {
                        service.awaitTermination(3, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new RuntimeException(lastSubmittedException);
                }
            }

            errorCounter = 0;

            if (chunks.isEmpty()) {
                log.info("All chunks are processed");
                break;
            }

//            printMemInfo();
        } while (true);

        service.shutdown();
        service.close();

        dropChunkTable(configs);
        if (targetStorage instanceof JDBCStorage) {
            targetStorage.dropOutboxTable(false);
        }
    }
*/


    @Override
    public void fulfillChunks(List<Config> configs, boolean sync, int rows) throws SQLException {
        CqlSession cqlSession = csPool.getCqlSession();
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
        log.info("Token range size: {}", trs.size());

        for (Config c : configs) {
            CSTable sourceTable = new CSTable(c.fromSchemaName(), c.fromTableName(), null, null);
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
/*
                        String chunk = getConnectionProperty() == null ? oTable(null) :
                                oTable(getConnectionProperty().getFromProperty());
*/
                        PreparedStatement ps = cqlSession.prepare(DML_INSERT_CHUNK_TABLE.replace("$tableName", getOutboxTable().tableToString()));
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

    private long getEstimatedRowsInRange(CqlSession cqlSession, CSTable sourceTable, TokenRange tr) {
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
                             long stop, CSTable sourceTable, String taskName, long shift) {
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
    public void dropChunkTable(List<Config> configs) throws SQLException {
        CqlSession cqlSession = csPool.getCqlSession();
/*
        String table = getConnectionProperty() == null ? oTable(null) :
                oTable(getConnectionProperty().getFromProperty());
*/
        try {
            cqlSession.execute(DDL_DROP_TABLE.replace("$tableName", getOutboxTable().tableToString()));
        } catch (Exception e) {
            log.warn("Chunk table {} not found", getOutboxTable().tableToString());
        }
    }

    @Override
    public void createGlobalOutbox() throws SQLException {
        CqlSession cqlSession = csPool.getCqlSession();
/*
        String chunk = getConnectionProperty() == null ? oTable(null) :
                oTable(getConnectionProperty().getToProperty());
*/
        cqlSession.execute(DDL_CREATE_GLOBAL_OUTBOX_TABLE.replace("$tableName", getOutboxTable().tableToString()));
        log.info("Outbox table {} created successfully", getOutboxTable().tableToString());
    }

    @Override
    public boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk) {
        CqlSession cqlSession = (CqlSession) chunk.getTargetSession();
/*
        String chunkTable = getConnectionProperty() == null ? oTable(null) :
                oTable(getConnectionProperty().getToProperty());
*/
        String selectCQL = DML_SELECT_OUTBOX_TABLE.replace("$tableName", getOutboxTable().tableToString());
        com.datastax.oss.driver.api.core.cql.ResultSet rs = cqlSession.execute(selectCQL, chunk.getId());
        return rs.one() != null;
    }

    @Override
    public void insertProcessedChunkInfo(Chunk<?, ?, ?, ?> chunk) {
        CqlSession cqlSession = (CqlSession) chunk.getTargetSession();
/*
        String chunkTable = getConnectionProperty() == null ? oTable(null) :
                oTable(getConnectionProperty().getToProperty());
*/
        String insertCQL = DML_INSERT_OUTBOX_TABLE.replace("$tableName", getOutboxTable().tableToString());
        PreparedStatement statement = cqlSession.prepare(insertCQL);
        BoundStatement boundStatement = statement.bind(chunk.getId(), chunk.getConfig().fromTaskName(), chunk.getCopied())
                .setPageSize(1_000)
                .setTimeout(Duration.ofSeconds(1))
                .setConsistencyLevel(ConsistencyLevel.QUORUM);
        cqlSession.execute(boundStatement);
    }

    @Override
    public void dropOutboxTable(boolean sync) throws SQLException {
/*
        String chunk = getConnectionProperty() == null ? oTable(null) :
                oTable(getConnectionProperty().getToProperty());
*/
        try {
            CqlSession cqlSession = csPool.getCqlSession();
            cqlSession.execute(DDL_DROP_TABLE.replace("$tableName", getOutboxTable().tableToString()));
        } catch (Exception e) {
            log.info("Outbox table {} not found, nothing to drop", getOutboxTable().tableToString());
        }
    }

    @Override
    public void createChunkTable() {
        CqlSession cqlSession = csPool.getCqlSession();
        cqlSession.execute(DDL_CREATE_CHUNK_TABLE.replace("$tableName", getOutboxTable().tableToString()));
    }

/*
    private String oTable(Properties properties) {
        String outboxTable = "";
        if (getOutboxTable().getSchemaName() == null || properties != null) {
            String keyspace = properties.getProperty("keyspace");
            outboxTable = keyspace + "." + getOutboxTable().getTableName();
        } else {
            outboxTable = getOutboxTable().tableToString();
        }
        return outboxTable;
    }
*/

    @Override
    public String buildStartEndOfChunk(Config config, Table sourceTable) {
/*
        String chunk = getConnectionProperty() == null ? oTable(null) :
                oTable(getConnectionProperty().getFromProperty());
*/
        return "select chunk_id, start_page, end_page, task_name, schema_name, table_name, status from " +
                getOutboxTable().tableToString() + " where " +
                "status in ('ASSIGNED', 'UNASSIGNED', 'PROCESSED_WITH_ERROR') " +
                " and schema_name = ? and table_name = ? " +
                " per partition limit 100 ";
    }

    @Override
    public List<Chunk<?, ?, ?, ?>> getChunkList(List<Config> configs, Storage targetStorage) throws SQLException {
        List<Chunk<?, ?, ?, ?>> chunks = new ArrayList<>();
        CqlSession sourceSession = getSession();
//        log.info("Get chunk list from {}", chunkTableName);
        configs.forEach(config -> {
            Table sourceTable = this.configToTable(config.fromSchemaName(), config.fromTableName());
            Table targetTable = targetStorage.configToTable(config.toSchemaName(), config.toTableName());
            try {
                this.enrichTable(sourceTable);
                targetStorage.enrichTable(sourceTable, targetTable);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
//            targetTable.getColumns().forEach(c -> System.out.println(c.columnName() + "." + c.columnType()));
            String orderByClause = targetTable.buildOrderBy(config);
            List<Column2Column> c2c = getColumn2Column(sourceTable, targetTable, config);
            Table2Table t2t = getTable2Table(sourceTable, targetTable, c2c, config);
            String sql = buildStartEndOfChunk(config, sourceTable);
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
                    Chunk<?, ?, ?, ?> chunk =
                            new CSChunk<>(
                                    row.getUuid("chunk_id"),
                                    row.getLong("start_page"),
                                    row.getLong("end_page"),
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
    public Table2Table getTable2Table(Table sourceTable,
                                      Table targetTable,
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
        return new Table2Table(sourceTable, targetTable, c2c, ttlColumn, timestampColumn);
    }

    @Override
    public List<Column2Column> getColumn2Column(Table sourceTable, Table targetTable, Config config) {
        List<Column2Column> column2Column = new ArrayList<>();
        if ((config.columnToColumn() == null || config.columnToColumn().isEmpty()) &&
                (config.expressionToColumn() == null || config.expressionToColumn().isEmpty())) {
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
        if ((config.columnToColumn() != null && !config.columnToColumn().isEmpty()) &&
                (config.avroSchema() == null || config.avroSchema().isEmpty())) {
            for (Map.Entry<String,String> entry : config.columnToColumn().entrySet()) {
                Column sourceColumn = sourceTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes().equals(entry.getKey().replace("\"", "")))
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
        if ((config.expressionToColumn() != null && !config.expressionToColumn().isEmpty()) &&
                (config.avroSchema() == null || config.avroSchema().isEmpty())) {
            for (Map.Entry<String,String> entry : config.expressionToColumn().entrySet()) {
                Column column = targetTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes().equals(entry.getValue().replace("\"", "")))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(entry.getValue() + " not found in target table " +
                                targetTable.getSchemaName() + "." + targetTable.getTableName()));
                column2Column.add(new Column2Column(column, column, entry.getKey()));
            }
        }
        int avroFieldPosition = 1;
        if ((config.columnToColumn() != null && !config.columnToColumn().isEmpty()) &&
                (config.avroSchema() != null && !config.avroSchema().isEmpty())) {
            for (Map.Entry<String, String> entry : config.columnToColumn().entrySet()) {
                Column sourceColumn = sourceTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes()
                                .equalsIgnoreCase(entry.getKey().replace("\"", "")))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(entry.getKey() + " not found in source table " +
                                sourceTable.getSchemaName() + "." + sourceTable.getTableName()));
                Column targetColumn = columnFromAvro(config.avroSchema(), entry.getValue(), avroFieldPosition++);
                column2Column.add(new Column2Column(sourceColumn, targetColumn, null));
            }
        }
        if ((config.expressionToColumn() != null && !config.expressionToColumn().isEmpty()) &&
                (config.avroSchema() != null && !config.avroSchema().isEmpty())) {
            for (Map.Entry<String, String> entry : config.expressionToColumn().entrySet()) {
                Column targetColumn = columnFromAvro(config.avroSchema(), entry.getValue(), avroFieldPosition++);
                column2Column.add(new Column2Column(targetColumn, targetColumn, entry.getKey()));
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
        if (isManaged) {
            csPool.closeCqlSession();
            log.info("Cassandra target storage stopped");
        }
    }

    @Override
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?, ?, ?> chunk) {
        return Map.of();
    }

    @Override
    public Map<Table, Table> configsToTables(List<Config> configs, Storage targetStorage) {
        return Map.of();
    }

    @Override
    public Table configToTable(String schemaName, String tableName) {
        return new CSTable(schemaName, tableName, null, null);
    }

    @Override
    public Table getTargetTableBySourceTable(Table table) {
        return null;
    }

    @Override
    public Table getSourceTableByTargetTable(Table table) {
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
    public String buildFetchStatement(Config config, Table2Table t2t) {
        CSTable sourceTable = (CSTable) t2t.sourceTable();
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
        if (t2t.ttlColumn() == null) {
            List<String> ttlColumns = columns
                    .stream()
                    .filter(s -> pkColumns.stream().noneMatch(pk -> pk.columnName().equals(s)))
                    .filter(s -> ckColumns.stream().noneMatch(ck -> ck.columnName().equals(s)))
                    .filter(s -> nonStaticColumns.stream().anyMatch(ns -> ns.columnName().equals(s)))
                    .filter(s -> nonFrozenCollectionColumns.stream().noneMatch(nfc -> nfc.columnName().equals(s)))
                    .map(s -> "ttl(" + s + ")")
                    .toList();
            columns.addAll(ttlColumns);
        }
        if (t2t.timestampColumn() == null) {
            List<String> timestampColumns = columns
                    .stream()
                    .filter(s -> pkColumns.stream().noneMatch(pk -> pk.columnName().equals(s)))
                    .filter(s -> ckColumns.stream().noneMatch(ck -> ck.columnName().equals(s)))
                    .filter(s -> nonStaticColumns.stream().anyMatch(ns -> ns.columnName().equals(s)))
                    .filter(s -> nonFrozenCollectionColumns.stream().noneMatch(nfc -> nfc.columnName().equals(s)))
                    .map(s -> "writetime(" + s + ")")
                    .toList();
            columns.addAll(timestampColumns);
        }
        String columnToColumn = String.join(", ", columns);
        return PGKeywords.SELECT + " " +
                columnToColumn + " " +
//                (t2t.ttlColumn() == null ? "" : ( t2t.ttlColumn().defaultValue().equals("NULL")  ?  (" , (int)NULL as  \""  + t2t.ttlColumn().columnName() + "\" ") : (", cast((int)0 + " + t2t.ttlColumn().defaultValue() + " as int ) as \"" + t2t.ttlColumn().columnName() + "\" ")  ) ) +
//                (t2t.ttlColumn() == null ? "" : ( t2t.ttlColumn().defaultValue().equals("NULL")  ?  (" , (int)NULL as  \""  + t2t.ttlColumn().columnName() + "\" ") : (", " + t2t.ttlColumn().defaultValue() + " as \"" + t2t.ttlColumn().columnName() + "\" ")  ) ) +
                getTtlColumnClause(t2t) +
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

    private String getTtlColumnClause(Table2Table t2t) {
        return t2t.ttlColumn() == null ? "" :
                (t2t.ttlColumn().defaultValue().equals("NULL") ? (" , (int)NULL as  \"" + t2t.ttlColumn().columnName() + "\" ") : (", " + t2t.ttlColumn().defaultValue() + " as \"" + t2t.ttlColumn().columnName() + "\" "));
    }

    @Override
    public <S extends AutoCloseable> void setSession(S session) {
    }

    @Override
    public void enrichTable(Table sourceTable) throws SQLException {
        sourceTable.enrichTable(getSession());
    }

    @Override
    public void enrichTable(Table sourceTable, Table targetTable) throws SQLException {
        targetTable.enrichTable(getSession());
    }

    @Override
    public <K, T, S extends AutoCloseable, R, V> void insertColumnValue(List<ColumnValue<V>> columnValues,
                                                                           Chunk<K, T, S, R> chunk) {
    }

    @Override
    public <K, T, S extends AutoCloseable, R> void closeWriter(Chunk<K, T, S, R> chunk, String tableName) {

    }

    @Override
    public <K, T, S extends AutoCloseable, R, W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        return null;
    }

    @Override
    public <K, T, S extends AutoCloseable, R> void flushBuffer(Chunk<K, T, S, R> chunk) {

    }

    @Override
    public void preChecks(List<Config> configs) throws SQLException {

    }
}
