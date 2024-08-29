package org.bublik;

import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.service.StorageService;
import org.bublik.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

import static org.bublik.exception.Utils.getStackTrace;

public class Bublik {
    private static final Logger LOGGER = LoggerFactory.getLogger(Bublik.class);
    private final List<Config> configs;
    private final ConnectionProperty connectionProperty;

    private Bublik(ConnectionProperty connectionProperty, List<Config> configs) {
        this.connectionProperty = connectionProperty;
        this.configs = configs;
    }

    public void start() {
        LOGGER.info("Bublik starting...");
        try {
            Storage sourceStorage = StorageService.getStorage(connectionProperty.getFromProperty(), connectionProperty, true);
            assert sourceStorage != null;
            sourceStorage.start(configs);
            LOGGER.info("All Bublik's tasks have been done.");
        } catch (SQLException e) {
            LOGGER.error("{}", getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    public static Bublik getInstance(ConnectionProperty connectionProperty, List<Config> configs) {
        return new Bublik(connectionProperty, configs);
    }
}