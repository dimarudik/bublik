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
    private final Table2Table<S> t2t;
    private final String fetchQuery;
    private final Storage<K, T, S, R> sourceStorage;
    private long startTime;
    private final Storage<K, T, S, R> targetStorage;
    private S sourceSession;
    private S targetSession;
    private LogMessage logMessage;
    private R resultSet;
    private int copied;
    private String batchInsertQuery;
    private int upserted;
    private ChunkStatus chunkStatus;

    public Chunk(K id, T start, T end, Config config, Table2Table<S> t2t, ChunkStatus status,
                 String fetchQuery, Storage<K, T, S, R> sourceStorage, Storage<K, T, S, R> targetStorage) {
        this.id = id;
        this.start = start;
        this.end = end;
        this.config = config;
        this.t2t = t2t;
        this.chunkStatus = status;
        this.fetchQuery = fetchQuery;
        this.sourceStorage = sourceStorage;
        this.targetStorage = targetStorage;
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

    public Table2Table<S> getT2t() {
        return t2t;
    }

    public Storage<K, T, S, R> getSourceStorage() {
        return sourceStorage;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public Storage<K, T, S, R> getTargetStorage() {
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

    public String getFetchQuery() {
        return fetchQuery;
    }

    public int getCopied() {
        return copied;
    }

    public void setCopied(int copied) {
        this.copied = copied;
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

    public void setChunkStatus(ChunkStatus chunkStatus) {
        this.chunkStatus = chunkStatus;
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

    public void logChunkInfo() {
        log.info("{} {}\t {} sec",
                getLogMessage().operation(),
                this,
                Math.round((float) (getLogMessage().stop() - getLogMessage().start()) / 10) / 100.0);
    }

    @Override
    public String toString() {
        String toTableName = t2t.targetTable() == null ? "" : " -> " + t2t.targetTable().getTableName();
        return  t2t.sourceTable().getTableName() +
                toTableName +
                " of " + copied +
                " rows (start:" + getStart() +
                ", end:" + getEnd() +
                ") chunk_id:" + getId();
    }
}
