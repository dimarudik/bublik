package org.chekist;

import org.bublik.secure.EncryptedEntity;
import org.bublik.secure.SecureHandler;
import org.bublik.secure.SecureConfig;
import org.bublik.secure.SecureData;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class ChekistHandler extends SecureHandler {
    @Override
    public SecureConfig getSecureConfig(String fileName) {
        return new ChekistConfig(fileName);
    }

    @Override
    public SecureData getSecureData(String keyEncryptionKey, String plainText, String aad) {
        return new ChekistData(keyEncryptionKey, plainText, aad);
    }

    @Override
    public <C extends SecureConfig, D extends SecureData> EncryptedEntity getEncryptedEntity(C c, D d) {
        try {
            return new ChekistEntity((ChekistConfig) c, (ChekistData) d);
        } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException | NoSuchPaddingException |
                 InvalidKeyException | IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }
/*
    @Override
    public String decrypt(SecureConfig config) {
        return "";
    }
*/
}
