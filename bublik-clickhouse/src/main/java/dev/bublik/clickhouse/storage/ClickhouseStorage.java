package dev.bublik.clickhouse.storage;

import com.clickhouse.client.api.Client;
import dev.bublik.core.model.Chunk;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.LogMessage;
import dev.bublik.core.storage.StorageClass;

import java.sql.SQLException;
import java.util.List;

public class ClickhouseStorage<K, T, S extends Client, R> extends ClickStorage<K, T, S, R> {
    public ClickhouseStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
    }

    @Override
    public LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        return null;
    }

    @Override
    public void initCache(List<Config> configs) throws SQLException {
        log.info("Cache is not supported for MSSQL");
    }
}
