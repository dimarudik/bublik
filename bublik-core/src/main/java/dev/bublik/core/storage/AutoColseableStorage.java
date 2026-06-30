package dev.bublik.core.storage;

import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.Table;

public abstract class AutoColseableStorage extends Storage {
    protected AutoColseableStorage(StorageClass storageClass, ConnectionProperty connectionProperty, Table outboxTable) {
        super(storageClass, connectionProperty, outboxTable);
    }
}
