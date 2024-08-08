package org.bublik.storage.cassandraaddons;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.bublik.storage.cassandraaddons.MM3.defaultTokenRange;

public class MM3Batch {
    private final Map<TokenRange, BatchEntity> tokenRangeMap = new HashMap<>();

    private MM3Batch() {
    }

    public Map<TokenRange, BatchEntity> getTokenRangeMap() {
        return tokenRangeMap;
    }

    public void putTokenRangeMap(TokenRange tokenRange) {
        BatchEntity batchEntity = getCounter(tokenRange);
        if (batchEntity == null) {
            batchEntity = new BatchEntity(0, BatchStatement.builder(DefaultBatchType.LOGGED));
        } else {
            int counter = batchEntity.getCounter() + 1;
            batchEntity.setCounter(counter);
        }
        tokenRangeMap.put(tokenRange, batchEntity);
    }

    private BatchEntity getCounter(TokenRange tokenRange) {
        return getTokenRangeMap().get(tokenRange);
    }

    public static MM3Batch initMM3Batch(Set<TokenRange> tokenRangeSet) {
        MM3Batch mm3Batch = new MM3Batch();
        mm3Batch.putTokenRangeMap(defaultTokenRange());
        tokenRangeSet.forEach(mm3Batch::putTokenRangeMap);
        return mm3Batch;
    }

    public static class BatchEntity {
        private Integer counter;
        private final BatchStatementBuilder batchStatementBuilder;

        public BatchEntity(Integer counter, BatchStatementBuilder batchStatementBuilder) {
            this.counter = counter;
            this.batchStatementBuilder = batchStatementBuilder;
        }

        public Integer getCounter() {
            return counter;
        }

        public void setCounter(Integer counter) {
            this.counter = counter;
        }

        public BatchStatementBuilder getBatchStatementBuilder() {
            return batchStatementBuilder;
        }
    }
}
