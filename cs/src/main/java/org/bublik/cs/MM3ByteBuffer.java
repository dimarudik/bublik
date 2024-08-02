package org.bublik.cs;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;
import org.apache.cassandra.dht.Murmur3Partitioner;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MM3ByteBuffer {
    public static void main(String[] args) {
        byte[] stringBytes = "27".getBytes(StandardCharsets.UTF_8);
        byte[] intBytes = toBytes(27);

        byte[] cKey = new byte[stringBytes.length + intBytes.length];
        System.arraycopy(intBytes, 0, cKey, 0, intBytes.length);
        System.arraycopy(stringBytes, 0, cKey, intBytes.length, stringBytes.length);

        ByteBuffer bbString = ByteBuffer.wrap(stringBytes);
        ByteBuffer bbInt = ByteBuffer.wrap(intBytes);
        ByteBuffer bbcKey = ByteBuffer.wrap(cKey);

        Murmur3TokenFactory murmur3TokenFactory = new Murmur3TokenFactory();

        Murmur3Token tokenString = (Murmur3Token) murmur3TokenFactory.hash(bbString);
        System.out.println(tokenString.getValue());

        Murmur3Token tokenInt = (Murmur3Token) murmur3TokenFactory.hash(bbInt);
        System.out.println(tokenInt.getValue());

        Murmur3Token tokenCKey = (Murmur3Token) murmur3TokenFactory.hash(bbcKey);
        System.out.println(tokenCKey.getValue());

        // https://stackoverflow.com/questions/22915237/how-to-generate-cassandra-token-for-composite-partition-key
/*
        double i = (((Math.pow(2,64) / 2) * tokenInt.getValue()) - Math.pow(2, 63)) -
                (((Math.pow(2,64) / 2) * tokenString.getValue()) - Math.pow(2, 63));

        System.out.println(i);
*/
//        (((2**64 / 2) * i) - 2**63) for i in range(number_of_tokens)

        Murmur3Partitioner murmur3Partitioner = new Murmur3Partitioner();
        Murmur3Partitioner.LongToken stringToken = murmur3Partitioner.getToken(bbString);
        Murmur3Partitioner.LongToken longToken = murmur3Partitioner.getToken(bbInt);

        System.out.println(stringToken.token);
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
