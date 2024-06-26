package org.bublik.storage;

import com.datastax.driver.core.Cluster;

import java.util.Properties;

public class CassandraStorageClass extends StorageClass {
    public CassandraStorageClass(Class<Cluster> aClass, Properties properties) {
        super(aClass, properties);
    }
}
