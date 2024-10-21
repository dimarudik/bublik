package org.bublik.cs;

import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class MM3ByteBuffer {
    public static void main(String[] args) {
        Murmur3TokenFactory murmur3TokenFactory = new Murmur3TokenFactory();

        byte[] tsBytes = timestampToBytes(LocalDateTime.parse("09:15:30 AM, Sun 10/30/2022",
                        DateTimeFormatter.ofPattern("hh:mm:ss a, EEE M/d/uuuu")).toInstant(ZoneOffset.UTC));
        ByteBuffer bbTsKey = ByteBuffer.wrap(tsBytes);
        Murmur3Token tokenTsKey = (Murmur3Token) murmur3TokenFactory.hash(bbTsKey);
        System.out.println(tokenTsKey.getValue() + " timestamp");

        byte[] uuidBytes = uuidToBytes(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        ByteBuffer bbUuidKey = ByteBuffer.wrap(uuidBytes);
        Murmur3Token tokenUuidKey = (Murmur3Token) murmur3TokenFactory.hash(bbUuidKey);
        System.out.println(tokenUuidKey.getValue() + " uuid");

        byte[] smallIntBytes = smallIntToBytes(1000);
        ByteBuffer bbSmallIntKey = ByteBuffer.wrap(smallIntBytes);
        Murmur3Token tokenSmallIntKey = (Murmur3Token) murmur3TokenFactory.hash(bbSmallIntKey);
        System.out.println(tokenSmallIntKey.getValue() + " smallint");

        byte[] intBytes = intToBytes(1000);
        ByteBuffer bbIntKey = ByteBuffer.wrap(intBytes);
        Murmur3Token tokenIntKey = (Murmur3Token) murmur3TokenFactory.hash(bbIntKey);
        System.out.println(tokenIntKey.getValue());

        byte[] longBytes = longToBytes(1000);
        ByteBuffer bbLongKey = ByteBuffer.wrap(longBytes);
        Murmur3Token tokenLongKey = (Murmur3Token) murmur3TokenFactory.hash(bbLongKey);
        System.out.println(tokenLongKey.getValue());

        byte[] stringBytes = stringToBytes("Max");
        ByteBuffer bbStringKey = ByteBuffer.wrap(stringBytes);
        Murmur3Token tokenStringKey = (Murmur3Token) murmur3TokenFactory.hash(bbStringKey);
        System.out.println(tokenStringKey.getValue());

        byte[] cKey = compositeToBytes(longBytes, uuidBytes);
        ByteBuffer bbcKey = ByteBuffer.wrap(cKey);
        Murmur3Token tokenCKey = (Murmur3Token) murmur3TokenFactory.hash(bbcKey);
        System.out.println(tokenCKey.getValue());
    }

    private static byte[] smallIntToBytes(int i) {
        byte[] result = new byte[2];
        result[0] = (byte) (i >> 8);
        result[1] = (byte) (i /*>> 0*/);
        return result;
    }

    // https://stackoverflow.com/questions/1936857/convert-integer-into-byte-array-java
    private static byte[] intToBytes(int i) {
        byte[] result = new byte[4];
        result[0] = (byte) (i >> 24);
        result[1] = (byte) (i >> 16);
        result[2] = (byte) (i >> 8);
        result[3] = (byte) (i /*>> 0*/);
        return result;
    }

    // https://stackoverflow.com/questions/40087926/what-is-the-byte-size-of-common-cassandra-data-types-to-be-used-when-calculati
    private static byte[] longToBytes(long l) {
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

    private static byte[] stringToBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] timestampToBytes(Instant instant) {
        long l = instant.toEpochMilli();
        ByteBuffer bb = ByteBuffer.allocate(8);
        return longToBytes(l);
    }

    public static byte[] uuidToBytes(UUID uuid) {
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

    private static byte[] compositeToBytes(byte[]... bytes) {
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
