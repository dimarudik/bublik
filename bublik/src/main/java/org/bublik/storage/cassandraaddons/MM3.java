package org.bublik.storage.cassandraaddons;

import com.datastax.driver.core.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class MM3 {
    private final Object key;

    public MM3(Object key) {
        this.key = key;
    }

    // select token(id), id, "Primary" from target where id = 27;
    public Murmur3Token getMurmur3Token() {
        Murmur3TokenFactory murmur3TokenFactory = new Murmur3TokenFactory();
        byte[] bytes = new byte[0];
        if (key instanceof Number) {
            bytes = intToBytes(((Number) key).intValue());
        }
        if (key instanceof String) {
            bytes = stringToBytes(key.toString());
        }
        ByteBuffer bb = ByteBuffer.wrap(bytes);
/*
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
*/
        return (Murmur3Token) murmur3TokenFactory.hash(bb);
    }

    public TokenRange getTokenRange(Set<TokenRange> tokenRangeSet) {
        try {
            return tokenRangeSet
                    .stream()
                    .filter(tRange -> (Long) tRange.getStart().getValue() >= this.getMurmur3Token().getValue() &&
                            this.getMurmur3Token().getValue() < (Long) tRange.getStart().getValue())
                    .findAny()
                    .orElseThrow();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // https://stackoverflow.com/questions/1936857/convert-integer-into-byte-array-java
    private byte[] intToBytes(int i) {
        byte[] result = new byte[4];
        result[0] = (byte) (i >> 24);
        result[1] = (byte) (i >> 16);
        result[2] = (byte) (i >> 8);
        result[3] = (byte) (i /*>> 0*/);
        return result;
    }

    private byte[] stringToBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] compositeToBytes(byte[]... bytes) {
        int l = 0;
        int o = 3;
        int p = 0;
        for (byte[] b : bytes) {
            l += b.length + o;
        }
        byte[] r = new byte[l];
        for (byte[] b : bytes) {
            System.arraycopy(b, 0, r, p + o - 1, b.length);
            byte[] t = intToBytes(b.length);
            r[p] = t[o - 1];
            r[p + 1] = t[o];
            p += b.length + o;
        }
        return r;
    }
}
