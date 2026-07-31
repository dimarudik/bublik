package dev.bublik.core.storage;

import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.model.*;
import dev.bublik.core.service.Source;
import dev.bublik.core.service.StorageService;
import dev.bublik.core.service.Target;

import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static dev.bublik.core.util.Utils.getStackTrace;

public abstract class Storage implements StorageService, Wrapper, AutoCloseable, Source, Target {
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
                            try {
                                log.warn("Saving info about error to database");
                                chunk.interStageSaveChunkStatus(ChunkStatus.PROCESSED_WITH_ERROR, false, null, getStackTrace(e), getOutboxTable().tableToString());
                            } catch (SQLException ex) {
                                log.error("Error while saving info about error to database. ChunkId = {} {}.{} {}", chunk.getId(), chunk.getT2t().sourceTable().getSchemaName(), chunk.getT2t().sourceTable().getTableName(), getStackTrace(ex));
                            }
                            if (this instanceof JDBCStorage) {
                                try {
                                    (chunk.getSourceSession()).close();
                                    log.warn("Source session has been closed due to error");
                                } catch (SQLException ex) {
                                    log.error("Error while closing source session. ChunkId = {} {}.{} {}", chunk.getId(), chunk.getT2t().sourceTable().getSchemaName(), chunk.getT2t().sourceTable().getTableName(), getStackTrace(ex));
                                }
                            }
                            if (targetStorage instanceof JDBCStorage) {
                                try {
                                    (chunk.getTargetSession()).close();
                                    log.warn("Target session has been closed due to error");
                                } catch (SQLException ex) {
                                    log.error("Error while closing target session. ChunkId = {} {}.{} {}", chunk.getId(), chunk.getT2t().sourceTable().getSchemaName(), chunk.getT2t().sourceTable().getTableName(), getStackTrace(ex));
                                }
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

            printMemInfo();
        } while (true);

        service.shutdown();
        service.close();

        dropChunkTable(configs);
        if (targetStorage instanceof JDBCStorage) {
            targetStorage.dropOutboxTable(false);
        }
    }

    private void printMemInfo() {
        Runtime runtime = Runtime.getRuntime();
        long byteToMb = 1024L * 1024L;
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        log.info("=================== MEMORY INFO =========================");
        log.info("Max Heap Size (-Xmx):   {} MB", maxMemory == Long.MAX_VALUE ? "Unlimited" : maxMemory / byteToMb);
        log.info("Allocated Heap Size:    {} MB", totalMemory / byteToMb);
        log.info("Used Heap Memory:       {} MB", usedMemory / byteToMb);
        log.info("Free Heap Memory:       {} MB", (maxMemory - usedMemory) / byteToMb);
        log.info("==========================================================");
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
