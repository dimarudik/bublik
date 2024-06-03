package org.example.util;

import org.example.constants.SourceContextHolder;
import org.example.exception.TableNotExistsException;
import org.example.model.Chunk;
import org.example.model.Config;
import org.example.model.RunnerResult;
import org.example.model.Table;
import org.example.service.TableService;
import org.example.task.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.example.constants.SQLConstants.LABEL_ORACLE;
import static org.example.constants.SQLConstants.LABEL_POSTGRESQL;
import static org.example.util.ColumnUtil.fillCtidChunks;
import static org.example.util.ColumnUtil.getChunkMap;

public class Bublik {
    private static Bublik INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(Bublik.class);
    private final List<Config> configs;
    private final ExecutorService executorService;

    private final List<Future<RunnerResult>> futures = new ArrayList<>();

    private Bublik(int threads, List<Config> configs){
        this.configs = configs;
        this.executorService = Executors.newFixedThreadPool(threads);
    }

    public void initiateProcessFromDatabase(Boolean initPGChunks,
                                            Boolean copyPGChunks) {
        try (Connection connection = DatabaseUtil.getConnectionDbFrom()) {
            SourceContextHolder sourceContextHolder = DatabaseUtil.sourceContextHolder(connection);
            if (sourceContextHolder.sourceContext().toString().equals(LABEL_ORACLE)){
                initiateTargetThread(connection, configs);
            } else if (sourceContextHolder.sourceContext().toString().equals(LABEL_POSTGRESQL)){
                if (initPGChunks) {
                    fillCtidChunks(connection, configs);
                }
                if (copyPGChunks) {
                    initiateTargetThread(connection, configs);
                } else {
                    return;
                }
            } else return;
            futureProceed(futures);

            executorService.shutdown();
        } catch (SQLException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
            executorService.shutdown();
        }
    }

    private void futureProceed(List<Future<RunnerResult>> tasks) throws InterruptedException, ExecutionException {
        Iterator<Future<RunnerResult>> futureIterator = tasks.listIterator();
        while (futureIterator.hasNext()) {
            Future<RunnerResult> future = futureIterator.next();
            if (future.isDone()) {
                RunnerResult runnerResult = future.get();
                LOGGER.info("{} {}\t {} sec",
                        runnerResult.logMessage().operation(),
                        runnerResult.logMessage(),
                        Math.round((float) (System.currentTimeMillis() - runnerResult.logMessage().start()) / 10) / 100.0);
                futureIterator.remove();
            }
            if (!futureIterator.hasNext()) {
                futureIterator = tasks.listIterator();
            }
                Thread.sleep(1);
        }
    }

    private void initiateTargetThread(Connection connection,
                                      List<Config> configs) throws SQLException, InterruptedException {
        Map<Integer, Chunk<?>> chunkMap = new TreeMap<>(getChunkMap(connection,configs));
        for (Map.Entry<Integer, Chunk<?>> i : chunkMap.entrySet()) {
            Table table = TableService.getTable(connection, i.getValue().getConfig().fromSchemaName(), i.getValue().getConfig().fromTableName());
            if (table.exists(connection)) {
                Map<String, Integer> orderedColumns = new HashMap<>();
                i.getValue().getConfig().columnToColumn().forEach((k, v) -> orderedColumns.put(k, null));
                futures.add(executorService.submit(new Worker(i.getValue(), orderedColumns)));
            } else {
                throw new TableNotExistsException("Table "
                        + i.getValue().getSourceTable().getSchemaName() + "."
                        + i.getValue().getSourceTable().getTableName() + " does not exist.");
            }
        }
    }

    public static synchronized Bublik getInstance(int threads, List<Config> configs) {
        if(INSTANCE == null) {
            INSTANCE = new Bublik(threads, configs);
        }
        return INSTANCE;
    }
}