package dev.bublik.core.storage;

import dev.bublik.core.model.ConnectionProperty;

public abstract class AutoColseableStorage extends Storage {
    protected AutoColseableStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
    }
}
