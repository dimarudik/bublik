package org.bublik.cs;

import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MM3ByteBuffer {
    public static void main(String[] args) {
        byte[] stringBytes = stringToBytes("Some text with spaces and commas + ..........................................................................");
        byte[] intBytes = intToBytes(100);
        byte[] cKey = compositeToBytes(stringBytes, intBytes);
        ByteBuffer bbcKey = ByteBuffer.wrap(cKey);
        Murmur3TokenFactory murmur3TokenFactory = new Murmur3TokenFactory();
        Murmur3Token tokenCKey = (Murmur3Token) murmur3TokenFactory.hash(bbcKey);
        System.out.println(tokenCKey.getValue());
    }

    private static byte[] intToBytes(int i) {
        byte[] result = new byte[4];
        result[0] = (byte) (i >> 24);
        result[1] = (byte) (i >> 16);
        result[2] = (byte) (i >> 8);
        result[3] = (byte) (i /*>> 0*/);
        return result;
    }

    private static byte[] stringToBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
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
