package dev.bublik.core.service;

import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.storage.Storage;

public interface StorageFactory {
    Storage<?,?,?,?> create(ConnectionProperty connectionProperty);
}
