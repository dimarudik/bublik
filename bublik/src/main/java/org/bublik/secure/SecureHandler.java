package org.bublik.secure;

public abstract class SecureHandler {
    public abstract SecureConfig getSecureConfig(String fileName);
    public abstract SecureData getSecureData(String keyEncryptionKey, String plainText, String aad);
    public abstract <C extends SecureConfig, D extends SecureData> EncryptedEntity getEncryptedEntity(C c, D d);
}
