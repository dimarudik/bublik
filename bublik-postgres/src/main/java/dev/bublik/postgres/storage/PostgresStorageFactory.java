package dev.bublik.postgres.storage;

import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageFactory;
import dev.bublik.core.storage.Storage;
import dev.bublik.postgres.model.PGTable;

import java.sql.SQLException;
import java.util.Properties;

public class PostgresStorageFactory implements StorageFactory {
    @Override
    public boolean supports(Properties properties) {
        String url = properties.getProperty("url");
        return url != null && (url.startsWith("jdbc:postgresql:") || url.startsWith("jdbc:humus:"));
    }

    @Override
    public Storage create(Properties properties,
                          ConnectionProperty connectionProperty,
                          Table table,
                          boolean isSource) throws SQLException {
        if (table == null) {
            String schemaName;
            String tableName;
            if (isSource) {
                schemaName = properties.getProperty("chunkSchemaName") == null ? "public" : properties.getProperty("chunkSchemaName");
                tableName = properties.getProperty("chunkTableName") == null ? "bublik_chunk" : properties.getProperty("chunkTableName");
            } else {
                schemaName = properties.getProperty("outboxSchemaName") == null ? "public" : properties.getProperty("outboxSchemaName");
                tableName = properties.getProperty("outboxTableName") == null ? "bublik_outbox" : properties.getProperty("outboxTableName");
            }
            return new PostgresStorage(properties, connectionProperty, PostgresStorage.getDefaultTableName(schemaName, tableName));
        }
        return new PostgresStorage(properties, connectionProperty, table);
    }
}
