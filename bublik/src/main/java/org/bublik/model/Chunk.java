package org.bublik.model;

import org.bublik.service.ChunkService;

public abstract class Chunk<T> implements ChunkService {
    private final Integer id;
    private final T start;
    private final T end;
    private final Config config;
    private final Table sourceTable;
    private final Table targetTable;

    public Chunk(Integer id, T start, T end, Config config, Table sourceTable, Table targetTable) {
        this.id = id;
        this.start = start;
        this.end = end;
        this.config = config;
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
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
}
