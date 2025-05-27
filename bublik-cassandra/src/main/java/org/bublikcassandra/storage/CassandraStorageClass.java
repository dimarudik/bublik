package org.bublikcassandra.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import org.bublik.storage.StorageClass;

import java.util.Properties;

public class CassandraStorageClass extends StorageClass {
    public CassandraStorageClass(Class<CqlSession> aClass, Properties properties) {
        super(aClass, properties);
    }
}
