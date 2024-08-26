package org.bublik.storage;

import com.datastax.oss.driver.api.core.CqlSession;

import java.util.Properties;

public class CassandraStorageClass extends StorageClass {
    public CassandraStorageClass(Class<CqlSession> aClass, Properties properties) {
        super(aClass, properties);
    }
}
