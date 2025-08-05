package org.bublik.storage;

import org.bublik.model.ConnectionProperty;
import org.bublik.model.Table;
import org.bublik.service.StorageService;

import java.util.Map;

public abstract class Storage implements StorageService {
    private final StorageClass storageClass;
    private final ConnectionProperty connectionProperty;
    private final Boolean isSource;
    private Map<Table, Table> tables;

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

    public Boolean getSource() {
        return isSource;
    }

    public Map<Table, Table> getTables() {
        return tables;
    }

    public void setTables(Map<Table, Table> tables) {
        this.tables = tables;
    }
}
