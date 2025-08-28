package org.bublik.core.storage;

import java.sql.Connection;
import java.util.Properties;

public class JDBCStorageClass extends StorageClass {
    public JDBCStorageClass(Class<Connection> aClass, Properties properties) {
        super(aClass, properties);
    }
}
