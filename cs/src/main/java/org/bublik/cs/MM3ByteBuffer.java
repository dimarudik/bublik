package org.bublik.cs;

import org.apache.cassandra.dht.Murmur3Partitioner;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MM3ByteBuffer {
    public static void main(String[] args) {
        Murmur3Partitioner murmur3Partitioner = new Murmur3Partitioner();
//        Murmur3Partitioner.LongToken longToken = murmur3Partitioner.getToken(ByteBuffer.wrap(Long.toString(27L).getBytes(StandardCharsets.UTF_8)));
        Murmur3Partitioner.LongToken longToken = murmur3Partitioner.getToken(ByteBuffer.wrap(toBytes(256)));

        System.out.println(longToken.token);


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

    public static byte[] intToByteArray(int value) {
        if ((value >>> 24) > 0) {
            return new byte[]{
                    (byte) (value >>> 24),
                    (byte) (value >>> 16),
                    (byte) (value >>> 8),
                    (byte) value
            };
        } else if ((value >> 16) > 0) {
            return new byte[]{
                    (byte) (value >>> 16),
                    (byte) (value >>> 8),
                    (byte) value
            };
        } else if ((value >> 8) > 0) {
            return new byte[]{
                    (byte) (value >>> 8),
                    (byte) value
            };
        } else {
            return new byte[]{
                    (byte) value
            };
        }
    }
}
