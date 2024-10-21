package org.bublik.storage.cassandraaddons;

import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenRange;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public class MM3 {

    public static Murmur3Token getMurmur3Token(byte[] bytes) {
        Murmur3TokenFactory murmur3TokenFactory = new Murmur3TokenFactory();
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return (Murmur3Token) murmur3TokenFactory.hash(bb);
    }

    public static TokenRange getTokenRange(Set<TokenRange> tokenRangeSet, byte[] bytes) {
        long v = getMurmur3Token(bytes).getValue();
        return tokenRangeSet
                .stream()
                .filter(tRange ->   v >= ((Murmur3Token)tRange.getStart()).getValue() &&
                                    v < ((Murmur3Token)tRange.getEnd()).getValue())
                .findFirst()
                .orElse(defaultTokenRange());
    }

    public static TokenRange defaultTokenRange() {
        return new Murmur3TokenRange(Murmur3TokenFactory.MIN_TOKEN, Murmur3TokenFactory.MAX_TOKEN);
    }

    public static TokenRange defaultTokenRange(long v){
        System.out.println(v + " = " + Murmur3TokenFactory.MIN_TOKEN + ":" + Murmur3TokenFactory.MAX_TOKEN);
        return new Murmur3TokenRange(Murmur3TokenFactory.MIN_TOKEN, Murmur3TokenFactory.MAX_TOKEN);
    }

    public static byte[] smallIntToBytes(int i) {
        byte[] result = new byte[2];
        result[0] = (byte) (i >> 8);
        result[1] = (byte) (i /*>> 0*/);
        return result;
    }

    // https://stackoverflow.com/questions/1936857/convert-integer-into-byte-array-java
    public static byte[] intToBytes(int i) {
        byte[] result = new byte[4];
        result[0] = (byte) (i >> 24);
        result[1] = (byte) (i >> 16);
        result[2] = (byte) (i >> 8);
        result[3] = (byte) (i /*>> 0*/);
        return result;
    }

    public static byte[] longToBytes(long l) {
        byte[] result = new byte[8];
        result[0] = (byte) (l >> 56);
        result[1] = (byte) (l >> 48);
        result[2] = (byte) (l >> 40);
        result[3] = (byte) (l >> 32);
        result[4] = (byte) (l >> 24);
        result[5] = (byte) (l >> 16);
        result[6] = (byte) (l >> 8);
        result[7] = (byte) (l /*>> 0*/);
        return result;
    }

    public static byte[] stringToBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] timestampToBytes(Instant instant) {
        long l = instant.toEpochMilli();
        return longToBytes(l);
    }

    public static byte[] uuidToBytes(UUID uuid) {
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

    public static byte[] compositeToBytes(byte[]... bytes) {
        if (bytes.length == 1)
            return bytes[0];
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
