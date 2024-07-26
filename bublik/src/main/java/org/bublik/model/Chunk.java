package org.bublik.model;

import org.bublik.service.ChunkService;
import org.bublik.storage.Storage;

import java.sql.Connection;
import java.sql.ResultSet;

public abstract class Chunk<T> implements ChunkService {
    private final Integer id;
    private final T start;
    private final T end;
    private final Config config;
    private final Table sourceTable;
    private Table targetTable;
    private final Storage sourceStorage;
    private final Storage targetStorage;
    private Connection sourceConnection;
    private LogMessage logMessage;
    private ResultSet resultSet;

    public Chunk(Integer id, T start, T end, Config config, Table sourceTable,
                 Storage sourceStorage, Storage targetStorage) {
        this.id = id;
        this.start = start;
        this.end = end;
        this.config = config;
        this.sourceTable = sourceTable;
        this.sourceStorage = sourceStorage;
        this.targetStorage = targetStorage;
    }

    public Integer getId() {
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

    public Storage getTargetStorage() {
        return targetStorage;
    }

    public Connection getSourceConnection() {
        return sourceConnection;
    }

    public void setSourceConnection(Connection sourceConnection) {
        this.sourceConnection = sourceConnection;
    }

    public LogMessage getLogMessage() {
        return logMessage;
    }

    public void setLogMessage(LogMessage logMessage) {
        this.logMessage = logMessage;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }
}
