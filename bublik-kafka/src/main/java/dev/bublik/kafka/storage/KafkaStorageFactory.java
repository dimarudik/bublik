package dev.bublik.kafka.storage;

import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageFactory;
import dev.bublik.core.storage.Storage;

import java.sql.SQLException;
import java.util.Properties;

public class KafkaStorageFactory implements StorageFactory {
    @Override
    public boolean supports(Properties properties) {
        String className = properties.getProperty("class");
        return className != null && "dev.bublik.kafka.storage.KafkaStorage".equals(className.trim());
    }

    @Override
    public Storage create(Properties properties, ConnectionProperty connectionProperty, Table table) throws SQLException {
        return new KafkaStorage(properties, connectionProperty, table);
    }
}
