package org.bublik;

import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.service.StorageService;
import org.bublik.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

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
        Storage sourceStorage, targetStorage;
        try {
            sourceStorage = StorageService.getStorage(connectionProperty.getFromProperty(), connectionProperty);
            assert sourceStorage != null;
            sourceStorage.startWorker(configs);
            targetStorage = StorageService.get();
            // переделать на NullPointerException
/*
            try {
                targetStorage.closeStorage();
            } catch (NullPointerException e) {
                e.printStackTrace();
            }
*/
            if (targetStorage != null) {
                targetStorage.closeStorage();
            }
            LOGGER.info("All Bublik's tasks have been done.");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Bublik getInstance(ConnectionProperty connectionProperty, List<Config> configs) {
        return new Bublik(connectionProperty, configs);
    }
}