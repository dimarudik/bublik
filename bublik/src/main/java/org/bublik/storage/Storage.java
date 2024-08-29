package org.bublik.storage;

import org.bublik.model.ConnectionProperty;
import org.bublik.service.StorageService;

public abstract class Storage implements StorageService {
    private final StorageClass storageClass;
    private final ConnectionProperty connectionProperty;
    private final Boolean isSource;

    protected Storage(StorageClass storageClass, ConnectionProperty connectionProperty, Boolean isSource) {
        this.storageClass = storageClass;
        this.connectionProperty = connectionProperty;
        this.isSource = isSource;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public ConnectionProperty getConnectionProperty() {
        return connectionProperty;
    }

    public Boolean getIsSource() {
        return isSource;
    }
}
