package org.bublik.core.storage;

import org.bublik.core.model.ConnectionProperty;

public abstract class AutoColseableStorage extends Storage {
    protected AutoColseableStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
    }
}
