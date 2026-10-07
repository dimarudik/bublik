package dev.bublik.clickhouse.storage;

import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageFactory;
import dev.bublik.core.storage.Storage;

import java.sql.SQLException;
import java.util.Properties;

public class ClickHouseStorageFactory implements StorageFactory {
    @Override
    public boolean supports(Properties properties) {
        String className = properties.getProperty("class");
        return className != null && "dev.bublik.clickhouse.storage.ClickHouseStorage".equals(className.trim());
    }

    @Override
    public Storage create(Properties properties, ConnectionProperty connectionProperty, Table table) throws SQLException {
        return new ClickHouseStorage(properties, connectionProperty, table);
    }
}
