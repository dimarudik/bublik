package dev.bublik.core.storage;

import java.util.Properties;

public class AutoColseableStorageClass extends StorageClass {
    public AutoColseableStorageClass(Class<AutoCloseable> aClass, Properties properties) {
        super(aClass, properties);
    }
}
