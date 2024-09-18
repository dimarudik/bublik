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

public class BubSecure {

    public static void main(String[] args) throws NoSuchAlgorithmException, NoSuchPaddingException,
            InvalidAlgorithmParameterException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException,
            InvalidKeySpecException {

        // Шифруем
        byte[] ivText = generateVector(12);
        byte[] ivDek = generateVector(12);

        byte[] dek = serializeSecretKey(generateSecretKey(256));
        byte[] kek = serializeSecretKey(generateSecretKey(256));
        byte[] aad = "WORD".getBytes();

        Cipher dekCipher = initCipher(dek, ivText, Cipher.ENCRYPT_MODE);
        dekCipher.updateAAD(aad);
        byte[] encryptedTextBytes = dekCipher.doFinal("PAN".getBytes());

        Cipher kekCipher = initCipher(kek, ivDek, Cipher.ENCRYPT_MODE);
        byte[] encryptedDekBytes = kekCipher.doFinal(dek);


        // Дешифруем
        byte[] ivText1 = decode(encode(ivText));
        byte[] ivDek1 = decode(encode(ivDek));

        Cipher cipherKek = initCipher(kek, ivDek1, Cipher.DECRYPT_MODE);
        byte[] dek1 = cipherKek.doFinal(encryptedDekBytes);

        Cipher cipherDek = initCipher(dek1, ivText1, Cipher.DECRYPT_MODE);
        cipherDek.updateAAD("WORD".getBytes());

        byte[] textBytes = cipherDek.doFinal(decode(encode(encryptedTextBytes)));

        System.out.println(new String(textBytes));
    }

    public static byte[] generateVector(int vectorSize) {
        byte[] iv = new byte[vectorSize];
        new SecureRandom().nextBytes(iv);
        return iv;
    }

    public static byte[] serializeSecretKey(SecretKey secretKey) {
        return secretKey.getEncoded();
    }

    public static SecretKey generateSecretKey(int keySize) throws NoSuchAlgorithmException {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(keySize, new SecureRandom());
        return keyGen.generateKey();
    }

    private static Cipher initCipher(byte[] secretKeyBytes, byte[] ivPt, int cryptMode) throws NoSuchPaddingException,
            NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException {
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(128, ivPt);
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
