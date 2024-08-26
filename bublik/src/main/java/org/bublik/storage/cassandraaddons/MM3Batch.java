package org.bublik.storage.cassandraaddons;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
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

    public void putTokenRange(TokenRange tokenRange, BatchEntity batchEntity) {
        tokenRangeMap.put(tokenRange, batchEntity);
    }

    public BatchEntity getBatchEntity(TokenRange tokenRange) {
        return getTokenRangeMap().get(tokenRange);
    }

/*
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
*/

    public static MM3Batch initMM3Batch(Set<TokenRange> tokenRangeSet) {
        MM3Batch mm3Batch = new MM3Batch();
        TokenRange defaultTokenRange = defaultTokenRange();
        mm3Batch.putTokenRange(defaultTokenRange, new BatchEntity(BatchStatement.builder(DefaultBatchType.LOGGED)));
        tokenRangeSet.forEach(v -> mm3Batch.putTokenRange(v, new BatchEntity(BatchStatement.builder(DefaultBatchType.LOGGED))));
        return mm3Batch;
    }
}
