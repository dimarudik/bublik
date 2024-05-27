package org.example.util;

import lombok.extern.slf4j.Slf4j;
import org.example.constants.SourceContextHolder;
import org.example.exception.TableNotExistsException;
import org.example.model.*;
import org.example.service.TableService;
import org.example.task.Worker;

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

@Slf4j
public class ProcessUtil {

    public void initiateProcessFromDatabase(List<Config> configs,
                                            Integer threads,
                                            Boolean initPGChunks,
                                            Boolean copyPGChunks) {
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        try (Connection connection = DatabaseUtil.getConnectionDbFrom()) {
            List<Future<LogMessage>> tasks = new ArrayList<>();
            SourceContextHolder sourceContextHolder = DatabaseUtil.sourceContextHolder(connection);
            if (sourceContextHolder.sourceContext().toString().equals(LABEL_ORACLE)){
                initiateTargetThread(connection, configs, executorService, tasks);
            } else if (sourceContextHolder.sourceContext().toString().equals(LABEL_POSTGRESQL)){
                if (initPGChunks) {
                    fillCtidChunks(connection, configs);
                }
                if (copyPGChunks) {
                    initiateTargetThread(connection, configs, executorService, tasks);
                } else {
                    return;
                }
            } else return;
            futureProceed(tasks);

            executorService.shutdown();
        } catch (SQLException | InterruptedException | ExecutionException e) {
            executorService.shutdown();
            log.error("Stopping all threads... {}", e.getMessage());
        }
    }

    private void futureProceed(List<Future<LogMessage>> tasks) throws InterruptedException, ExecutionException {
        Iterator<Future<LogMessage>> futureIterator = tasks.listIterator();
        while (futureIterator.hasNext()) {
            Future<LogMessage> future = futureIterator.next();
            if (future.isDone()) {
                LogMessage logMessage = future.get();
                futureIterator.remove();
            }
            if (!futureIterator.hasNext()) {
                futureIterator = tasks.listIterator();
            }
            Thread.sleep(1);
        }
    }

    private void initiateTargetThread(Connection connection,
                                      List<Config> configs,
                                      ExecutorService executorService,
                                      List<Future<LogMessage>> tasks) throws SQLException {
        Map<Integer, Chunk<?>> chunkMap = new TreeMap<>(getChunkMap(connection,configs));
        chunkMap.forEach((key, chunk) -> {
                try {
                    Table table = TableService.getTable(connection, chunk.getConfig().fromSchemaName(), chunk.getConfig().fromTableName());
                    if (table.exists(connection)) {
                        Map<String, Integer> orderedColumns = new HashMap<>();
                        chunk.getConfig().columnToColumn().forEach((k, v) -> orderedColumns.put(k, null));
                        tasks.add(executorService.submit(new Worker(chunk, orderedColumns)));
                    } else {
                        throw new TableNotExistsException("Table "
                                + chunk.getSourceTable().getSchemaName() + "."
                                + chunk.getSourceTable().getTableName() + " does not exist.");
                    }
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }
            }
        );
    }
}