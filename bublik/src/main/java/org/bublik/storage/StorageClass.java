package org.bublik.storage;

import java.util.Properties;

public abstract class StorageClass {
    private final Class<? extends AutoCloseable> aClass;
    private final Properties properties;

    public StorageClass(Class<? extends AutoCloseable> aClass, Properties properties) {
        this.aClass = aClass;
        this.properties = properties;
    }

    public Class<? extends AutoCloseable> getaClass() {
        return aClass;
    }

    public Properties getProperties() {
        return properties;
    }
}
