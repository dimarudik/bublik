package org.bublik.core.storage;

import org.bublik.core.model.ConnectionProperty;
import org.bublik.core.model.Table;
import org.bublik.core.service.StorageService;

import java.sql.Wrapper;
import java.util.Map;

public abstract class Storage implements StorageService, Wrapper {
    private final StorageClass storageClass;
    private final ConnectionProperty connectionProperty;
    private Map<Table, Table> tables;


    protected Storage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        this.storageClass = storageClass;
        this.connectionProperty = connectionProperty;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public ConnectionProperty getConnectionProperty() {
        return connectionProperty;
    }

    public Map<Table, Table> getTables() {
        return tables;
    }

    public void setTables(Map<Table, Table> tables) {
        this.tables = tables;
    }
}
