package org.bublik.storage.cassandraaddons;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.bublik.storage.cassandraaddons.MM3.defaultTokenRange;

public class MM3Batch {
    private static final Logger LOGGER = LoggerFactory.getLogger(MM3Batch.class);
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
    public static MM3Batch createMM3Batch() {
        return new MM3Batch();
    }

    public void initMM3Batch(Set<TokenRange> tokenRangeSet) {
        TokenRange defaultTokenRange = defaultTokenRange();
        this.putTokenRange(defaultTokenRange, new BatchEntity(BatchStatement.builder(DefaultBatchType.LOGGED)));
        tokenRangeSet.forEach(v -> this.putTokenRange(v, new BatchEntity(BatchStatement.builder(DefaultBatchType.LOGGED))));
    }
}
