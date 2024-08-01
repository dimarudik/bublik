package org.bublik.cs;

import org.apache.cassandra.dht.Murmur3Partitioner;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MM3ByteBuffer {
    public static void main(String[] args) {
        Murmur3Partitioner murmur3Partitioner = new Murmur3Partitioner();
        Murmur3Partitioner.LongToken longToken = murmur3Partitioner.getToken(ByteBuffer.wrap(Long.toString(27L).getBytes(StandardCharsets.UTF_8)));
        System.out.println(longToken.token);
        Murmur3Partitioner.LongToken l = new Murmur3Partitioner.LongToken(27l);
        System.out.println(l);
        System.out.println(murmur3Partitioner.getToken(Murmur3Partitioner.LongToken.keyForToken(27L)).token);

        System.out.println(BigInteger.valueOf((long) l.getTokenValue()));
        Murmur3Partitioner.LongToken ll = murmur3Partitioner.getToken(ByteBuffer.wrap(new byte[] {27}));
        System.out.println(ll.getTokenValue());

        long firstLong = 150;
        long secondLong = -130;

        // explicit type conversion from long to byte
        byte firstByte = (byte)firstLong;
        byte secondByte = (byte)secondLong;

        // printing typecasted value
        System.out.println(firstByte);
        System.out.println(secondByte);
    }
}
