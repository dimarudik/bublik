package org.bublik.cs.secure;

import javax.crypto.*;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/*
https://habr.com/ru/articles/444814/
*/

public class Chekist {
    private final byte[] saltKey;
    private final byte[] saltVector;
    private final byte[] encryptedSaltBytes;
    private final byte[] plainTextVector;
    private final String saltText;
    private final byte[] encryptedTextBytes;

    public Chekist(String saltText, String plainText) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException,
            NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        this.plainTextVector = generateVector(12);
        this.saltVector = generateVector(12);

        SecretKey plainTextSecretKey = generateSecretKey(256);
        SecretKey saltSecretKey = generateSecretKey(256);

        byte[] plainTextKey = serializeSecretKey(plainTextSecretKey);

        this.saltKey = serializeSecretKey(saltSecretKey);
        this.saltText = saltText;
        byte[] salt = saltText.getBytes();

        Cipher saltCipher = initCipher(saltKey, saltVector, Cipher.ENCRYPT_MODE);
        Cipher plainTextCipher = initCipher(plainTextKey, plainTextVector, Cipher.ENCRYPT_MODE);

        this.encryptedSaltBytes = saltCipher.doFinal(plainTextKey);
        plainTextCipher.updateAAD(salt);

        this.encryptedTextBytes = plainTextCipher.doFinal(plainText.getBytes());
    }

    public static void main(String[] args) throws NoSuchAlgorithmException, NoSuchPaddingException,
            InvalidAlgorithmParameterException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException,
            InvalidKeySpecException {

        String saltText = "WORD";
        String plainText = "SECRET BUBSECURE";

        // Шифруем
        long start = System.currentTimeMillis();
        List<Chekist> chekists = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            chekists.add(new Chekist(saltText, plainText));
        }
        System.out.println(System.currentTimeMillis() - start);

        //!!! ДОДЕЛАТЬ ENCODE DECODE

        // Дешифруем
        start = System.currentTimeMillis();
        chekists.forEach(chekist -> {
            byte[] decrypted1 = null;
            try {
                decrypted1 = chekist.decrypt();
            } catch (InvalidAlgorithmParameterException | NoSuchPaddingException | NoSuchAlgorithmException |
                     InvalidKeyException | IllegalBlockSizeException | BadPaddingException e) {
                throw new RuntimeException(e);
            }
//            System.out.println(new String(decrypted1));
        });
        System.out.println(System.currentTimeMillis() - start);
    }

    public byte[] decrypt()
            throws InvalidAlgorithmParameterException, NoSuchPaddingException, NoSuchAlgorithmException,
            InvalidKeyException, IllegalBlockSizeException, BadPaddingException {

        Cipher saltCipher = initCipher(saltKey, saltVector, Cipher.DECRYPT_MODE);
        byte[] dek = saltCipher.doFinal(encryptedSaltBytes);

        Cipher cipherDek = initCipher(dek, plainTextVector, Cipher.DECRYPT_MODE);
        cipherDek.updateAAD(saltText.getBytes());

        return cipherDek.doFinal(encryptedTextBytes);
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
