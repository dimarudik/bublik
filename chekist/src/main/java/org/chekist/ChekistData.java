package org.chekist;

import org.bublik.secure.SecureData;

public class ChekistData extends SecureData {
    private final String keyEncryptionKey;
    private final String plainText;
    private final String aad;

    public ChekistData(String keyEncryptionKey, String plainText, String aad) {
        this.keyEncryptionKey = keyEncryptionKey;
        this.plainText = plainText;
        this.aad = aad;
    }

    public String getKeyEncryptionKey() {
        return keyEncryptionKey;
    }

    public String getPlainText() {
        return plainText;
    }

    public String getAad() {
        return aad;
    }
}
