package org.bublik.storage.cassandraaddons;

import org.apache.cassandra.dht.Murmur3Partitioner;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MM3 {
    private final String key;

    public MM3(String key) {
        this.key = key;
    }

    public long getTokenLong() {
        Murmur3Partitioner murmur3Partitioner = new Murmur3Partitioner();
        Murmur3Partitioner.LongToken longToken = murmur3Partitioner.getToken(ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8)));
        return longToken.token;
    }
}
