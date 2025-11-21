package org.bublik.cassandra.storage.cassandraaddons;

public record CSValueAttribute(Integer ttl, Long timestamp) {

    @Override
    public String toString() {
        return "{" + ttl +
                ", " + timestamp +
                '}';
    }

    public boolean isEmpty() {
        return ttl == null && timestamp == null;
    }
}
