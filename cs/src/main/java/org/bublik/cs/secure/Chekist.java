package org.bublik.cs.secure;

import javax.crypto.*;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;

/*
https://habr.com/ru/articles/444814/
*/

public class Chekist {
    private final byte[] KEK;
    private final byte[] kekVector;
    private final byte[] encryptedDEKByKEK;
    private final byte[] dekVector;
    private final String aad;
    private final byte[] encryptedTextBytes;

    public Chekist(String keyEncryptionKey, String plainText, String aad) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException,
            NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        this.dekVector = generateVector(12);
        this.kekVector = generateVector(12);

        SecretKey dataEncryptionKey = generateSecretKey(256); // генерится рандомно

        byte[] DEK = serializeSecretKey(dataEncryptionKey);

        this.KEK = decode(keyEncryptionKey);
        this.aad = aad;
        byte[] aadBytes = aad.getBytes();

        Cipher kekCipher = initCipher(KEK, kekVector, Cipher.ENCRYPT_MODE);
        Cipher dekCipher = initCipher(DEK, dekVector, Cipher.ENCRYPT_MODE);

        this.encryptedDEKByKEK = kekCipher.doFinal(DEK);
        dekCipher.updateAAD(aadBytes);

        this.encryptedTextBytes = dekCipher.doFinal(plainText.getBytes());
    }

    public static void main(String[] args) throws NoSuchAlgorithmException, NoSuchPaddingException,
            InvalidAlgorithmParameterException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException,
            InvalidKeySpecException {

        String aad = "WORD";
        String plainText = "SECRET BUBSECURE";
        String keyEncryptionKey = "kreDbdMpeEu0Xt6q654exKGvRUw370H/NS0Tcmh+POc="; // мастер-ключ из vault

        // Шифруем
        Chekist chekist = new Chekist(keyEncryptionKey, plainText, aad);

        //!!! ДОДЕЛАТЬ ENCODE DECODE

        // Дешифруем
        byte[] decrypted = chekist.decrypt();
        System.out.println(new String(decrypted));
    }

    public byte[] decrypt()
            throws InvalidAlgorithmParameterException, NoSuchPaddingException, NoSuchAlgorithmException,
            InvalidKeyException, IllegalBlockSizeException, BadPaddingException {

        Cipher kekCipher = initCipher(KEK, kekVector, Cipher.DECRYPT_MODE);
        byte[] DEK = kekCipher.doFinal(encryptedDEKByKEK);

        Cipher dekCipher = initCipher(DEK, dekVector, Cipher.DECRYPT_MODE);
        dekCipher.updateAAD(aad.getBytes());

        return dekCipher.doFinal(encryptedTextBytes);
    }

    public static byte[] generateVector(int vectorSize) {
        byte[] vector = new byte[vectorSize];
        new SecureRandom().nextBytes(vector);
        return vector;
    }

    public static byte[] serializeSecretKey(SecretKey secretKey) {
        return secretKey.getEncoded();
    }

    public static SecretKey generateSecretKey(int keySize) throws NoSuchAlgorithmException {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(keySize, new SecureRandom());
        return keyGen.generateKey();
    }

    private static Cipher initCipher(byte[] secretKeyBytes, byte[] src, int cryptMode) throws NoSuchPaddingException,
            NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException {
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(128, src);
        SecretKey secretKey = deserializeSecretKey(secretKeyBytes);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(cryptMode, secretKey, gcmParameterSpec);
        return cipher;
    }

    public static SecretKey deserializeSecretKey(byte[] secretKey) {
        return new SecretKeySpec(secretKey, 0, secretKey.length, "AES");
    }

    public static byte[] decode(final String from) {
        return Base64.getDecoder().decode(from);
    }

    public static String encode(final byte[] from) {
        return Base64.getEncoder().encodeToString(from);
    }
}
