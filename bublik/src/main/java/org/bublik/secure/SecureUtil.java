package org.bublik.secure;

import org.bublik.model.ConnectionProperty;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

public class SecureUtil {
    public static String encrypt(ConnectionProperty property, String data) throws ClassNotFoundException, NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException {
        Properties properties = property.getCryptoProperties();
        Class<?> obj = Class.forName(properties.getProperty("handler"));
        Object chekist = obj.getDeclaredConstructor().newInstance();
        Class<?>[] classes = {String.class, String.class};
        Method method = chekist.getClass().getMethod("encrypt", classes);
        return (String) method.invoke(chekist, data, "");
    }
}
