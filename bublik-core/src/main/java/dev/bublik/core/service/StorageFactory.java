package dev.bublik.core.service;

import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.Table;
import dev.bublik.core.storage.Storage;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

public interface StorageFactory {
    boolean supports(Properties properties);
    Storage create(Properties properties,
                   ConnectionProperty connectionProperty,
                   Table table) throws SQLException;
    default String getVersion() {
        try (InputStream is = this.getClass().getResourceAsStream("/bublik-plugin.properties")) {
            if (is == null) return "unknown";
            Properties props = new Properties();
            props.load(is);
            return props.getProperty("version", "unknown");
        } catch (Exception e) {
            return "unknown";
        }
    }
}
