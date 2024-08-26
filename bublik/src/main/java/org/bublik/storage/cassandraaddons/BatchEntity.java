package org.bublik.storage.cassandraaddons;

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;

public class BatchEntity {
    private int counter = 0;
    private final BatchStatementBuilder batchStatementBuilder;

    public BatchEntity(BatchStatementBuilder batchStatementBuilder) {
        this.batchStatementBuilder = batchStatementBuilder;
    }

    public Integer getCounter() {
        return counter;
    }

    public BatchStatementBuilder getBatchStatementBuilder() {
        return batchStatementBuilder;
    }

    public void resetCounter() {
        counter = 0;
    }

    public void increaseCounter() {
        counter++;
    }
}
