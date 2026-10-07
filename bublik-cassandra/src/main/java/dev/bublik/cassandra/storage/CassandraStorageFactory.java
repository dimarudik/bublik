package dev.bublik.cassandra.storage;

import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageFactory;
import dev.bublik.core.storage.Storage;

import java.sql.SQLException;
import java.util.Properties;

public class CassandraStorageFactory implements StorageFactory {
    @Override
    public boolean supports(Properties properties) {
        String className = properties.getProperty("class");
        return className != null && "dev.bublik.cassandra.storage.CassandraStorage".equals(className.trim());
    }

    @Override
    public Storage create(Properties properties, ConnectionProperty connectionProperty, Table table) throws SQLException {
        return new CassandraStorage(properties, connectionProperty, table);
    }
}
