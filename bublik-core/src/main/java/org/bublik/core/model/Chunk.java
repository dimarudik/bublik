package org.bublik.core.model;

import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.service.ChunkService;
import org.bublik.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Chunk<K, T, S extends AutoCloseable, R> implements ChunkService<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(Chunk.class);

    private final K id;
    private final T start;
    private final T end;
    private final Config config;
    private final Table sourceTable;
    private final String fetchQuery;
    private final ChunkStatus chunkStatus;
    private Table targetTable;
    private final Storage sourceStorage;
    private long startTime;
    private Storage targetStorage;
    private S sourceSession;
    private S targetSession;
    private LogMessage logMessage;
    private R resultSet;
    private int rows;
    private String batchInsertQuery;
    private int upserted;

    public Chunk(K id, T start, T end, Config config, Table sourceTable,
                 ChunkStatus status, String fetchQuery, Storage sourceStorage) {
        this.id = id;
        this.start = start;
        this.end = end;
        this.config = config;
        this.sourceTable = sourceTable;
        this.chunkStatus = status;
        this.fetchQuery = fetchQuery;
        this.sourceStorage = sourceStorage;
    }

    public K getId() {
        return id;
    }

    public T getStart() {
        return start;
    }

    public T getEnd() {
        return end;
    }

    public Config getConfig() {
        return config;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public Storage getSourceStorage() {
        return sourceStorage;
    }

    public void setTargetTable(Table table) {
        this.targetTable = table;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public Storage getTargetStorage() {
        return targetStorage;
    }

    public LogMessage getLogMessage() {
        return logMessage;
    }

    public void setLogMessage(LogMessage logMessage) {
        this.logMessage = logMessage;
    }

    public R getResultSet() {
        return resultSet;
    }

    public void setResultSet(R resultSet) {
        this.resultSet = resultSet;
    }

    public void setTargetStorage(Storage targetStorage) {
        this.targetStorage = targetStorage;
    }

    public String getFetchQuery() {
        return fetchQuery;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

    public String getBatchInsertQuery() {
        return batchInsertQuery;
    }

    public void setBatchInsertQuery(String batchInsertQuery) {
        this.batchInsertQuery = batchInsertQuery;
    }

    public int getUpserted() {
        return upserted;
    }

    public void setUpserted(int upserted) {
        this.upserted = upserted;
    }

    public ChunkStatus getChunkStatus() {
        return chunkStatus;
    }

    public S getSourceSession() {
        return sourceSession;
    }

    public void setSourceSession(S sourceSession) {
        this.sourceSession = sourceSession;
    }

    public S getTargetSession() {
        return targetSession;
    }

    public void setTargetSession(S targetSession) {
        this.targetSession = targetSession;
    }
}
