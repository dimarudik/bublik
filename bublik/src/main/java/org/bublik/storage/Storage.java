package org.bublik.storage;

import org.bublik.model.ConnectionProperty;
import org.bublik.service.StorageService;

public abstract class Storage implements StorageService {
    private final StorageClass storageClass;
    private final ConnectionProperty connectionProperty;

    public Storage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        this.storageClass = storageClass;
        this.connectionProperty = connectionProperty;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public ConnectionProperty getConnectionProperty() {
        return connectionProperty;
    }
}
