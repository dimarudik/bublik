package org.bublik.cassandra.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import org.bublik.core.storage.StorageClass;

import java.util.Properties;

@Deprecated
public class CassandraStorageClass extends StorageClass {
    public CassandraStorageClass(Class<CqlSession> aClass, Properties properties) {
        super(aClass, properties);
    }
}
