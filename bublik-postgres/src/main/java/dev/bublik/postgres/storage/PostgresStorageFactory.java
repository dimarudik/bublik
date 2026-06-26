package dev.bublik.postgres.storage;

import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.service.StorageFactory;
import dev.bublik.core.storage.Storage;

public class PostgresStorageFactory implements StorageFactory {
    @Override
    public Storage create(ConnectionProperty connectionProperty) {
        return null;
/*
        try {
            return new JDBCPostgreSQLStorage(connectionProperty);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
*/
    }
}
