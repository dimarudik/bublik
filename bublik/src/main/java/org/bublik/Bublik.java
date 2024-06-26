package org.bublik;

import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.model.LogMessage;
import org.bublik.service.StorageService;
import org.bublik.storage.Storage;
import org.bublik.util.DatabaseUtil;
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
//    private static Bublik INSTANCE;
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
            DatabaseUtil.initializeConnectionPools(connectionProperty);
            assert sourceStorage != null;
            sourceStorage.initiateTargetThread(futures, configs, executorService);
            futureProceed(futures);
            executorService.shutdown();
            executorService.close();
            LOGGER.info("All Bublik's tasks have been done.");
//            targetStorage = StorageService.getStorage(connectionProperty.getToProperty());
//            Thread.sleep(10000);
        } catch (SQLException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }


/*
        try (Connection connection = DatabaseUtil.getPoolConnectionDbFrom()) {
            SourceContextHolder sourceContextHolder = DatabaseUtil.sourceContextHolder(connection);
            if (sourceContextHolder.sourceContext().toString().equals(LABEL_ORACLE)){
                initiateTargetThread(connection, configs, executorService);
            } else if (sourceContextHolder.sourceContext().toString().equals(LABEL_POSTGRESQL)){
                if (connectionProperty.getInitPGChunks()) {
                    fillCtidChunks(connection, configs);
                }
                if (connectionProperty.getCopyPGChunks()) {
                    initiateTargetThread(connection, configs, executorService);
                } else {
                    return;
                }
            } else return;
            futureProceed(futures);

            executorService.shutdown();
            executorService.close();
            DatabaseUtil.stopConnectionPools();
            LOGGER.info("All Bublik's tasks have been done.");
        } catch (SQLException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
            executorService.shutdown();
            try {
                DatabaseUtil.stopConnectionPools();
            } catch (SQLException ex) {
                LOGGER.error(e.getMessage(), e);
            }
        }
*/
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

/*
    private void initiateTargetThread(Connection connection,
                                      List<Config> configs,
                                      ExecutorService executorService) throws SQLException, InterruptedException {
        Map<Integer, Chunk<?>> chunkMap = new TreeMap<>(getChunkMap(connection,configs));
        for (Map.Entry<Integer, Chunk<?>> i : chunkMap.entrySet()) {
            Table table = TableService.getTable(connection, i.getValue().getConfig().fromSchemaName(), i.getValue().getConfig().fromTableName());
            if (table.exists(connection)) {
                Map<String, Integer> orderedColumns = new HashMap<>();
                i.getValue().getConfig().columnToColumn().forEach((k, v) -> orderedColumns.put(k, null));
                futures.add(executorService.submit(new Worker(i.getValue(), orderedColumns)));
            } else {
                LOGGER.error("\u001B[31mThe Source Table: {}.{} does not exist.\u001B[0m", i.getValue().getSourceTable().getSchemaName(),
                        i.getValue().getSourceTable().getTableName());
                throw new TableNotExistsException("The Source Table "
                        + i.getValue().getSourceTable().getSchemaName() + "."
                        + i.getValue().getSourceTable().getTableName() + " does not exist.");
            }
        }
    }
*/

    public static synchronized Bublik getInstance(ConnectionProperty connectionProperty, List<Config> configs) {
/*
        if(INSTANCE == null) {
            INSTANCE = new Bublik(connectionProperty, configs);
        }
        return INSTANCE;
*/
        return new Bublik(connectionProperty, configs);
    }
}