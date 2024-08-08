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

    public BatchEntity putTokenRange(TokenRange tokenRange) {
        BatchEntity batchEntity = getBatchEntity(tokenRange);
        if (batchEntity == null) {
            batchEntity = new BatchEntity(0, BatchStatement.builder(DefaultBatchType.LOGGED));
        } else {
            int counter = batchEntity.getCounter() + 1;
            batchEntity.setCounter(counter);
        }
        return tokenRangeMap.put(tokenRange, batchEntity);
    }

    private BatchEntity getBatchEntity(TokenRange tokenRange) {
        return getTokenRangeMap().get(tokenRange);
    }

    public BatchEntity getMaxBatchEntity() {
        Map.Entry<TokenRange, BatchEntity> maxEntry = null;
        for (Map.Entry<TokenRange, BatchEntity> entry : tokenRangeMap.entrySet()) {
            if (maxEntry == null || entry.getValue().getCounter().compareTo(maxEntry.getValue().getCounter()) > 0) {
                maxEntry = entry;
            }
        }
        assert maxEntry != null;
        return maxEntry.getValue();
    }

    public static MM3Batch initMM3Batch(Set<TokenRange> tokenRangeSet) {
        MM3Batch mm3Batch = new MM3Batch();
        mm3Batch.putTokenRange(defaultTokenRange());
        tokenRangeSet.forEach(mm3Batch::putTokenRange);
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
