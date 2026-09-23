package dev.bublik.core.storage;

import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.model.*;
import dev.bublik.core.service.Source;
import dev.bublik.core.service.StorageService;
import dev.bublik.core.service.Target;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static dev.bublik.core.util.Utils.getStackTrace;

public abstract class Storage implements StorageService, Wrapper, AutoCloseable, Source, Target {
    private static final System.Logger log = System.getLogger(Storage.class.getName());

    private final StorageClass storageClass;
    protected int threadCount;
    private final ConnectionProperty connectionProperty;
    protected Table outboxTable;
    private Map<Table, Table> tables;
    protected boolean isManaged;

    public Storage(ConnectionProperty connectionProperty) {
        this(null, connectionProperty, null);
    }

    public Storage(ConnectionProperty connectionProperty, Table outboxTable) {
        this(null, connectionProperty, outboxTable);
    }

    protected Storage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        this(storageClass, connectionProperty, null);
    }

    protected Storage(StorageClass storageClass, ConnectionProperty connectionProperty, Table outboxTable) {
        this.storageClass = storageClass;
        this.connectionProperty = connectionProperty;
        this.outboxTable = outboxTable;
    }

    protected Storage(Builder<?, ?> builder) {
        this.storageClass = null;
        this.connectionProperty = null;
        this.tables = null;
        this.threadCount = builder.threadCount;
        this.outboxTable = builder.outboxTable;
    }

    // Curiously Recurring Template Pattern (CRTP)
    protected static abstract class Builder<C extends Storage, B extends Builder<C, B>> {
        protected int threadCount;
        private Table outboxTable;

        protected abstract B self();

        public abstract C build();

        public B threadCount(int threadCount) {
            this.threadCount = threadCount;
            return self();
        }

        public B outboxTable(Table outboxTable) {
            this.outboxTable = outboxTable;
            return self();
        }

        protected void validate() {
        }
    }

    public Map<Table, Table> getTables() {
        return tables;
    }

    public void setTables(Map<Table, Table> tables) {
        this.tables = tables;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public ConnectionProperty getConnectionProperty() {
        return connectionProperty;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public Table getOutboxTable() {
        return outboxTable;
    }

    public void setOutboxTable(Table outboxTable) {
        this.outboxTable = outboxTable;
    }

    @Override
    public void start(Storage targetStorage, List<Config> cfgs, int rows) throws SQLException {
        List<Config> configs = copyConfigs(cfgs);
        if (getOutboxTable() == null || getOutboxTable().getTableName() == null) {
            setOutboxTable(getDefaultSourceOutboxTable());
            log.log(System.Logger.Level.INFO, "Chunk table is not set. Using default name: {0}", getOutboxTable().tableToString());
        }
        if (targetStorage.getOutboxTable() == null || targetStorage.getOutboxTable().getTableName() == null) {
            targetStorage.setOutboxTable(targetStorage.getDefaultTargetOutboxTable());
            log.log(System.Logger.Level.INFO, "Outbox table is not set. Using default name: {0}", targetStorage.getOutboxTable().tableToString());
        }
        if (rows > 0) {
            long lockId = Math.abs((long) (configs.getFirst().fromTaskName() + getOutboxTable().getTableName()).hashCode());
            if (tryDistributedLock(lockId)) {
                try {
                    log.log(System.Logger.Level.INFO,
                            "Winner is {0}. Starting initialization...", getPodName());
                    preChecks(configs);
                    createChunkTable();
                    fulfillChunks(configs, false, rows);
                    if (targetStorage instanceof JDBCStorage) {
                        targetStorage.createGlobalOutbox();
                    }
                } finally {
                    releaseDistributedLock(lockId);
                }
            } else {
                waitForTablesToExist(targetStorage);
            }
        }
        log.log(System.Logger.Level.INFO, "SOURCE version: {0}", getStorageVersion());
        log.log(System.Logger.Level.INFO, "TARGET version: {0}", targetStorage.getStorageVersion());
        int errorCounter = 0;
        log.log(System.Logger.Level.INFO, "THREADS: {0}", threadCount);
        log.log(System.Logger.Level.INFO, "FETCH_SIZE: {0}", getFetchSize());

        Throwable lastSubmittedException = null;
        List<TableMigrationContext> migrationContexts = getTableMigrationContexts(configs, targetStorage);

        do {
            List<Chunk<?, ?, ?, ?>> chunks = getChunkList(migrationContexts, targetStorage);
            if (chunks.isEmpty()) {
                log.log(System.Logger.Level.INFO, "All chunks are processed");
                break;
            }
            ExecutorService batchService = Executors.newFixedThreadPool(threadCount);
            List<Callable<Chunk<?, ?, ?, ?>>> tasks = new ArrayList<>();

            for (Chunk<?, ?, ?, ?> chunk : chunks) {
                tasks.add(() -> {
                    try {
                        return chunk.allStages(false, getOutboxTable());
                    } catch (Exception e) {
                        log.log(System.Logger.Level.ERROR, "ChunkId = {0} {1}.{2} failed", chunk.getId(),
                                chunk.getT2t().sourceTable().getSchemaName(),
                                chunk.getT2t().sourceTable().getTableName(), e);
                        try {
                            chunk.interStageSaveChunkStatus(ChunkStatus.PROCESSED_WITH_ERROR, false, null,
                                    getStackTrace(e), getOutboxTable().tableToString());
                        } catch (SQLException ex) {
                            log.log(System.Logger.Level.ERROR,
                                    "Error while saving error info for chunk {0}", chunk.getId(), ex);
                        }
                        if (this instanceof JDBCStorage) {
                            try {
                                (chunk.getSourceSession()).close();
                                log.log(System.Logger.Level.WARNING, "Source session has been closed due to error");
                            } catch (SQLException ex) {
                                log.log(System.Logger.Level.ERROR,
                                        "Error while closing source session. ChunkId = {0} {1}.{2} {3}",
                                        chunk.getId(), chunk.getT2t().sourceTable().getSchemaName(),
                                        chunk.getT2t().sourceTable().getTableName(), getStackTrace(ex));
                            }
                        }
                        if (targetStorage instanceof JDBCStorage) {
                            try {
                                (chunk.getTargetSession()).close();
                                log.log(System.Logger.Level.WARNING, "Target session has been closed due to error");
                            } catch (SQLException ex) {
                                log.log(System.Logger.Level.ERROR,
                                        "Error while closing target session. ChunkId = {0} {1}.{2} {3}",
                                        chunk.getId(), chunk.getT2t().sourceTable().getSchemaName(),
                                        chunk.getT2t().sourceTable().getTableName(), getStackTrace(ex));
                            }
                        }
                        throw new RuntimeException("ChunkId = " + chunk.getId() + " " + e.getMessage(), e);
                    }
                });
            }

            boolean hasBatchErrors = false;
            try {
                List<Future<Chunk<?, ?, ?, ?>>> futures = batchService.invokeAll(tasks);

                for (Future<?> future : futures) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        hasBatchErrors = true;
                        lastSubmittedException = (e.getCause() != null) ? e.getCause() : e;
                        errorCounter++;
                    }
                }
            } catch (InterruptedException e) {
                log.log(System.Logger.Level.ERROR, "Migration thread was interrupted", e);
                batchService.shutdownNow();
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                batchService.shutdown();
                try {
                    if (!batchService.awaitTermination(5, TimeUnit.MINUTES)) {
                        batchService.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    batchService.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }

            if (hasBatchErrors) {
                if (errorCounter > (threadCount * 2)) {
                    log.log(System.Logger.Level.ERROR,
                            "Critical error threshold reached ({0}/{1}). Finishing pipeline...",
                            errorCounter, threadCount * 2);
                    throw new RuntimeException("Unrecoverable error in migration pipeline", lastSubmittedException);
                } else {
                    log.log(System.Logger.Level.WARNING,
                            "Batch execution had errors. Total accumulated errors: {0}. Proceeding to next batch...", errorCounter);
                }
            } else {
                errorCounter = 0;
            }
            tasks.clear();
            chunks.clear();
            printMemInfo();
        } while (true);

        long cleanupLockId = Math.abs((long) (configs.getFirst().fromTaskName() + getOutboxTable().getTableName()).hashCode() + 999);
        if (tryDistributedLock(cleanupLockId)) {
            try {
                if (isMigrationFullyFinished()) {
                    log.log(System.Logger.Level.INFO,
                            "Pod {0} confirmed cluster completion. Dropping metadata tables...", getPodName());
                    dropChunkTable(configs);
                    if (targetStorage instanceof JDBCStorage) {
                        targetStorage.dropOutboxTable(false);
                    }
                    log.log(System.Logger.Level.INFO, "Infrastructure cleanup finished successfully.");
                } else {
                    log.log(System.Logger.Level.WARNING,
                            "Other pods are still processing chunks. Pod %s skips table deletion.", getPodName());
                }
            } finally {
                releaseDistributedLock(cleanupLockId);
            }
        }
    }

    private String getPodName() {
        String podName = System.getenv("HOSTNAME");
        return podName != null ? podName : "standalone-pod";
    }

    private void waitForTablesToExist(Storage targetStorage) throws SQLException {
        int attempts = 0;
        while (attempts < 300) {
            try {
                try (Connection sourceConnection = getPoolConnection();
                     Connection targetConnection = targetStorage.getPoolConnection()) {

                    if (getOutboxTable().exists(sourceConnection) && targetStorage.getOutboxTable().exists(targetConnection)) {
                        log.log(System.Logger.Level.INFO, "Infrastructure is verified and ready. Proceeding to migration.");
                        return;
                    }
                }
            } catch (Exception e) {}

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Infrastructure readiness polling interrupted", e);
            }
            attempts++;
        }
        throw new RuntimeException("Timeout exceeded for table creation by the Cluster Leader (10 min)!");
    }


    private List<TableMigrationContext> getTableMigrationContexts(List<Config> configs, Storage targetStorage) throws SQLException {
        List<TableMigrationContext> migrationContexts = new ArrayList<>();
        for (Config config : configs) {
            Table sourceTable = this.configToTable(config.fromSchemaName(), config.fromTableName());
            Table targetTable = targetStorage.configToTable(config.toSchemaName(), config.toTableName());

            this.enrichTable(sourceTable);
            targetStorage.enrichTable(sourceTable, targetTable);

            List<Column2Column> c2c = getColumn2Column(sourceTable, targetTable, config);
            Table2Table t2t = getTable2Table(sourceTable, targetTable, c2c, config);

            String chunkLookupSql = buildStartEndOfChunk(config, sourceTable);
            log.log(System.Logger.Level.DEBUG,
                    "Query of chunks for table {0}.{1}: {2}",
                    t2t.sourceTable().getSchemaName(), t2t.sourceTable().getTableName(), chunkLookupSql);
//            log.debug("Query of chunks for table {}.{}: {}", t2t.sourceTable().getSchemaName(), t2t.sourceTable().getTableName(), chunkLookupSql);
            String fetchQuery = buildFetchStatement(config, t2t);
            String orderByClause = targetTable.buildOrderBy(config);

            migrationContexts.add(new TableMigrationContext(config, t2t, chunkLookupSql, fetchQuery, orderByClause));
        }
        return migrationContexts;
    }

    private void printMemInfo() {
        Runtime runtime = Runtime.getRuntime();
        long byteToMb = 1024L * 1024L;
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        log.log(System.Logger.Level.INFO, "=================== MEMORY INFO =========================");
        log.log(System.Logger.Level.INFO, "Max Heap Size (-Xmx):   {0} MB", maxMemory == Long.MAX_VALUE ? "Unlimited" : maxMemory / byteToMb);
        log.log(System.Logger.Level.INFO, "Allocated Heap Size:    {0} MB", totalMemory / byteToMb);
        log.log(System.Logger.Level.INFO, "Used Heap Memory:       {0} MB", usedMemory / byteToMb);
        log.log(System.Logger.Level.INFO, "Free Heap Memory:       {0} MB", (maxMemory - usedMemory) / byteToMb);
        log.log(System.Logger.Level.INFO, "==========================================================");
//        log.info("=================== MEMORY INFO =========================");
//        log.info("Max Heap Size (-Xmx):   {} MB", maxMemory == Long.MAX_VALUE ? "Unlimited" : maxMemory / byteToMb);
//        log.info("Allocated Heap Size:    {} MB", totalMemory / byteToMb);
//        log.info("Used Heap Memory:       {} MB", usedMemory / byteToMb);
//        log.info("Free Heap Memory:       {} MB", (maxMemory - usedMemory) / byteToMb);
//        log.info("==========================================================");
    }

    public Column columnFromAvro(Map<String, Object> avroSchema, String avroFieldName, int position) {
        if (avroSchema == null || !avroSchema.containsKey("fields")) {
            throw new IllegalArgumentException("Invalid avroSchema structure: 'fields' block not found.");
        }

        List<Map<String, Object>> fields = (List<Map<String, Object>>) avroSchema.get("fields");

        Map<String, Object> avroField = fields.stream()
                .filter(f -> avroFieldName.equalsIgnoreCase(String.valueOf(f.get("name"))))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Field '" + avroFieldName + "' not found in avroSchema configuration"));

        Object typeObj = avroField.get("type");
        String columnType = "string";
        int isNullable = 1;

        if (typeObj instanceof List) {
            List<?> typeList = (List<?>) typeObj;
            isNullable = typeList.contains("null") ? 1 : 0;

            columnType = typeList.stream()
                    .map(String::valueOf)
                    .filter(t -> !"null".equalsIgnoreCase(t))
                    .findFirst()
                    .orElse("string");
        } else if (typeObj instanceof String) {
            columnType = (String) typeObj;
            isNullable = "null".equalsIgnoreCase(columnType) ? 1 : 0;
        }

        Object defaultObj = avroField.get("default");
        String defaultValue = (defaultObj != null) ? defaultObj.toString() : null;

        int dataType = java.sql.Types.VARCHAR; // Дефолт
        dataType = switch (columnType.toLowerCase()) {
            case "int" -> java.sql.Types.INTEGER;
            case "long" -> java.sql.Types.BIGINT;
            case "double" -> java.sql.Types.DOUBLE;
            case "float" -> java.sql.Types.FLOAT;
            case "boolean" -> java.sql.Types.BOOLEAN;
            case "bytes" -> java.sql.Types.BLOB;
            default -> dataType;
        };

        return new Column(
                position,
                avroFieldName,
                columnType,
                dataType,
                isNullable,
                defaultValue
        );
    }
}
