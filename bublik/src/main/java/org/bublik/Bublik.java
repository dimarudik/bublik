package org.bublik;

import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.model.LogMessage;
import org.bublik.service.StorageService;
import org.bublik.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Bublik {
    private static final Logger LOGGER = LoggerFactory.getLogger(Bublik.class);
    private final List<Config> configs;
    private final ConnectionProperty connectionProperty;

    private final List<Future<LogMessage>> futures = new ArrayList<>();

    private Bublik(ConnectionProperty connectionProperty, List<Config> configs) {
        this.connectionProperty = connectionProperty;
        this.configs = configs;
    }

    public void start() {
        ExecutorService executorService = Executors.newFixedThreadPool(connectionProperty.getThreadCount());
        LOGGER.info("Bublik starting...");
        Storage sourceStorage, targetStorage;
        try {
            sourceStorage = StorageService.getStorage(connectionProperty.getFromProperty(), connectionProperty);
            assert sourceStorage != null;
            sourceStorage.startWorker(futures, configs, executorService);
            futureProceed(futures);
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
            executorService.shutdown();
            executorService.close();
            LOGGER.info("All Bublik's tasks have been done.");
        } catch (SQLException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void futureProceed(List<Future<LogMessage>> tasks) throws InterruptedException, ExecutionException {
        Iterator<Future<LogMessage>> futureIterator = tasks.listIterator();
        while (futureIterator.hasNext()) {
            Future<LogMessage> future = futureIterator.next();
            if (future.isDone()) {
                LogMessage logMessage = future.get();
                LOGGER.info("{} {}\t {} sec",
                        logMessage.operation(),
                        logMessage,
                        Math.round((float) (System.currentTimeMillis() - logMessage.start()) / 10) / 100.0);
                futureIterator.remove();
            }
            if (!futureIterator.hasNext()) {
                futureIterator = tasks.listIterator();
            }
                Thread.sleep(1);
        }
    }

    public static Bublik getInstance(ConnectionProperty connectionProperty, List<Config> configs) {
        return new Bublik(connectionProperty, configs);
    }
}