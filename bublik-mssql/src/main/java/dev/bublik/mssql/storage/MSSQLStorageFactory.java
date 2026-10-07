package dev.bublik.mssql.storage;

import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageFactory;
import dev.bublik.core.storage.Storage;

import java.sql.SQLException;
import java.util.Properties;

public class MSSQLStorageFactory implements StorageFactory {
    @Override
    public boolean supports(Properties properties) {
        String url = properties.getProperty("url");
        return url != null && (url.startsWith("jdbc:sqlserver:"));
    }

    @Override
    public Storage create(Properties properties, ConnectionProperty connectionProperty, Table table) throws SQLException {
        return new MSSQLStorage(properties, connectionProperty, table);
    }
}
