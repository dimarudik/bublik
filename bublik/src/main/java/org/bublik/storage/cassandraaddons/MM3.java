package org.bublik.storage.cassandraaddons;

import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import org.apache.cassandra.dht.Murmur3Partitioner;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MM3 {
    private final Object key;

    public MM3(Object key) {
        this.key = key;
    }

    // select token(id), id, "Primary" from target where id = 27;
    public long getTokenLong() {
        Murmur3Partitioner murmur3Partitioner = new Murmur3Partitioner();
        Murmur3Partitioner.LongToken longToken = null;
        if (key instanceof String) {
            String keyString = key.toString();
            longToken = murmur3Partitioner.getToken(ByteBuffer.wrap(keyString.getBytes(StandardCharsets.UTF_8)));
        }
        if (key instanceof Number) {
            int keyInteger = ((Number) key).intValue();
            longToken = murmur3Partitioner.getToken(ByteBuffer.wrap(toBytes(keyInteger)));
        }
        assert longToken != null;
        return longToken.token;
    }

    public LongTokenRange getLongTokenRange(Set<TokenRange> tokenRangeSet) {
//        Map<TokenRange, LongTokenRange> map = new HashMap<>();
//        tokenRangeSet.forEach(t -> map.put(t, new LongTokenRange(t.getStart().getValue(), t.getEnd().getValue())));
//        Map<TokenRange, LongTokenRange> map = tokenRangeSet.stream().map(tokenRange -> );
        return tokenRangeSet
                .stream()
                .map(tokenRange -> new LongTokenRange(tokenRange.getStart().getValue(), tokenRange.getEnd().getValue()))
                .filter(l -> l.contains(this.getTokenLong()))
                .findAny()
                .orElse(new LongTokenRange(0L, 0L));
    }

    // https://stackoverflow.com/questions/1936857/convert-integer-into-byte-array-java
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
