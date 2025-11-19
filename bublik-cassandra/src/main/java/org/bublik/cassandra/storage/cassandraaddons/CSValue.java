package org.bublik.cassandra.storage.cassandraaddons;

import org.bublik.core.model.Column;

public record CSValue(Column column, Object value) {
}
