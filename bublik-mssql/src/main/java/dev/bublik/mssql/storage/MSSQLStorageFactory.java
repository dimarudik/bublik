package dev.bublik.mssql.storage;

import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.service.StorageFactory;
import dev.bublik.core.storage.Storage;

public class MSSQLStorageFactory implements StorageFactory {
    @Override
    public Storage<?, ?, ?, ?> create(ConnectionProperty connectionProperty) {
        return null;
    }
}
