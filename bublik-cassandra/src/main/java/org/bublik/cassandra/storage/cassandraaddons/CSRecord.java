package org.bublik.cassandra.storage.cassandraaddons;

import com.datastax.oss.driver.api.core.metadata.token.TokenRange;

public record CSRecord (TokenRange tokenRange,
                        Object[] values) {
}
