package dev.bublik.core.storage;

import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageService;

import java.sql.Wrapper;
import java.util.Map;

public abstract class Storage implements StorageService, Wrapper, AutoCloseable {
    private final StorageClass storageClass;
    protected int threadCount;
    private final ConnectionProperty connectionProperty;
    private Map<Table, Table> tables;

    public Storage(ConnectionProperty connectionProperty) {
        this.storageClass = null;
        this.connectionProperty = connectionProperty;
    }

    protected Storage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        this.storageClass = storageClass;
        this.connectionProperty = connectionProperty;
    }

    public Map<Table, Table> getTables() {
        return tables;
    }

    public void setTables(Map<Table, Table> tables) {
        this.tables = tables;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public ConnectionProperty getConnectionProperty() {
        return connectionProperty;
    }

    public int getThreadCount() {
        return threadCount;
    }
}
