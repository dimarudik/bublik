package org.bublik.storage.cassandraaddons;

import com.datastax.driver.core.TokenRange;
import org.apache.cassandra.dht.Murmur3Partitioner;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class MM3 {
    private final String key;

    public MM3(String key) {
        this.key = key;
    }

    // select token(id), id, "Primary" from target where id = 27;
    public long getTokenLong() {
        Murmur3Partitioner murmur3Partitioner = new Murmur3Partitioner();
        Murmur3Partitioner.LongToken longToken = murmur3Partitioner.getToken(ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8)));
//        Murmur3Partitioner.LongToken longToken = murmur3Partitioner.getToken(ByteBuffer.wrap(String.valueOf(27L).getBytes(StandardCharsets.UTF_8)));
//        Murmur3Partitioner.LongToken longToken = murmur3Partitioner.getToken(ByteBuffer.allocate(Long.BYTES).putLong(27));
//        Murmur3Partitioner.LongToken longToken = murmur3Partitioner.getToken(ByteBuffer.allocate(128).putLong(27L));
//        Murmur3Partitioner.LongToken longToken = murmur3Partitioner.getToken(ByteBuffer.wrap(ByteBuffer.allocate(Long.BYTES).putLong(27).array()));
//        Murmur3Partitioner.LongToken longToken = murmur3Partitioner.getToken(Murmur3Partitioner.LongToken.keyForToken(27));
        return longToken.token;
    }

    public LongTokenRange getLongTokenRange(Set<TokenRange> tokenRangeSet) {
        return tokenRangeSet
                .stream()
                .map(tokenRange -> new LongTokenRange(tokenRange.getStart().getValue(), tokenRange.getEnd().getValue()))
                .filter(l -> l.contains(this.getTokenLong()))
                .findAny()
                .orElseThrow();
    }

    private static byte[] toBytes(int i)
    {
        byte[] result = new byte[4];
        result[0] = (byte) (i >> 24);
        result[1] = (byte) (i >> 16);
        result[2] = (byte) (i >> 8);
        result[3] = (byte) (i /*>> 0*/);
        return result;
    }
}
