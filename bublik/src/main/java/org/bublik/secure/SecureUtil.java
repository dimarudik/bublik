package org.bublik.secure;

import org.bublik.model.ConnectionProperty;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

public class SecureUtil {
    public static EncryptedEntity getEncryptedEntity(ConnectionProperty property, String data, String aad) throws ClassNotFoundException, NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException {

        Properties properties = property.getCryptoProperties();

        SecureHandler secureHandler = getHandler(properties);
        SecureConfig secureConfig = secureHandler.getSecureConfig(properties.getProperty(SecuritySettings.cipherSettings.name()));
        SecureData secureData = secureHandler.getSecureData(properties.getProperty(SecuritySettings.keyEncryptionKey.name()), data, aad);

        return secureHandler.getEncryptedEntity(secureConfig, secureData);
    }

    private static SecureHandler getHandler(Properties properties) throws ClassNotFoundException, NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException {
        return (SecureHandler) getClazz(properties, SecuritySettings.handlerClass.name())
                .getDeclaredConstructor().newInstance();
    }

    private static Class<?> getClazz(Properties properties, String className) throws ClassNotFoundException {
        return Class.forName(properties.getProperty(className));
    }
}
