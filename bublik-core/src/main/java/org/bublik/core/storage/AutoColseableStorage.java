package org.bublik.core.storage;

import org.bublik.core.model.ConnectionProperty;

public abstract class AutoColseableStorage<K, T, S extends AutoCloseable, R> extends Storage<K, T, S, R> {
    protected AutoColseableStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
    }
}
