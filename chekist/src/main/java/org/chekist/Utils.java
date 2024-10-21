package org.chekist;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

public class Utils {
    public static byte[] decode(final String from) {
        return Base64.getDecoder().decode(from);
    }

    public static String encode(final byte[] from) {
        return Base64.getEncoder().encodeToString(from);
    }

    public static byte[] generateVector(int vectorSize) {
        byte[] vector = new byte[vectorSize];
        new SecureRandom().nextBytes(vector);
        return vector;
    }

    public static SecretKey generateSecretKey(int keySize, String algorithm) throws NoSuchAlgorithmException {
        KeyGenerator keyGen = KeyGenerator.getInstance(algorithm);
        keyGen.init(keySize, new SecureRandom());
        return keyGen.generateKey();
    }

    public static byte[] serializeSecretKey(SecretKey secretKey) {
        return secretKey.getEncoded();
    }

    public static SecretKey deserializeSecretKey(byte[] secretKey, String algorithm) {
        return new SecretKeySpec(secretKey, 0, secretKey.length, algorithm);
    }

    protected static Cipher initCipher(byte[] secretKeyBytes,
                                       byte[] src,
                                       int opMode,
                                       String algorithm,
                                       String transformation,
                                       int tLen) throws NoSuchPaddingException,
            NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException {
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(tLen, src);
        SecretKey secretKey = deserializeSecretKey(secretKeyBytes, algorithm);
        Cipher cipher = Cipher.getInstance(transformation);
        cipher.init(opMode, secretKey, gcmParameterSpec);
        return cipher;
    }
}
