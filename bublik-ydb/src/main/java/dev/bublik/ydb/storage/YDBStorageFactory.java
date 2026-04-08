package dev.bublik.ydb.storage;

import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.service.StorageFactory;
import dev.bublik.core.storage.Storage;

public class YDBStorageFactory implements StorageFactory {
    @Override
    public Storage<?, ?, ?, ?> create(ConnectionProperty connectionProperty) {
        return null;
    }
}
