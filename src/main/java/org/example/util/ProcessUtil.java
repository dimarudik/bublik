package org.example.util;

import lombok.extern.slf4j.Slf4j;
import org.example.constants.SourceContextHolder;
import org.example.exception.TableNotExistsException;
import org.example.model.ChunkDerpicated;
import org.example.model.Config;
import org.example.model.LogMessage;
import org.example.model.Table;
import org.example.service.LogMessageService;
import org.example.service.LogMessageServiceImpl;
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
import static org.example.util.ColumnUtil.*;

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
                Map<Integer, ChunkDerpicated> chunkMap = new TreeMap<>(getStartEndRowIdMap(connection, configs));
//                if (!chunkMap.isEmpty()) return;
                chunkMap.forEach((key, chunk) -> {
                        try {
                            Table table = TableService.getTable(connection, chunk.config().fromSchemaName(), chunk.config().fromTableName());
                            if (table.exists(connection)) {
                                Map<String, Integer> orderedColumns = new HashMap<>();
                                chunk.config().columnToColumn().forEach((k, v) -> orderedColumns.put(k, null));
                                tasks.add(
                                        executorService.
                                                submit(new Worker(
                                                        chunk,
                                                        orderedColumns
                                                ))
                                );
                            } else {
                                throw new TableNotExistsException("Table " + chunk.config().fromSchemaName() + "."
                                        + chunk.config().fromTableName() + " does not exist.");
                            }
                        } catch (SQLException e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                );
            } else if (sourceContextHolder.sourceContext().toString().equals(LABEL_POSTGRESQL)){
                if (initPGChunks) {
                    fillPGChunks(connection, configs);
                }
                if (copyPGChunks) {
                    Map<Integer, ChunkDerpicated> chunkMap = new TreeMap<>(getStartEndCTIDMap(connection, configs));
                    chunkMap.forEach((key, chunk) -> {
                            try {
                                Table table = TableService.getTable(connection, chunk.config().fromSchemaName(), chunk.config().fromTableName());
                                if (table.exists(connection)) {
                                    Map<String, Integer> orderedColumns = new HashMap<>();
                                    chunk.config().columnToColumn().forEach((k, v) -> orderedColumns.put(k, null));
                                    tasks.add(
                                            executorService.submit(new Worker(chunk, orderedColumns))
                                    );
                                } else {
                                    throw new TableNotExistsException("Table " + chunk.config().fromSchemaName() + "."
                                            + chunk.config().fromTableName() + " does not exist.");
                                }
                            } catch (SQLException e) {
                                log.error(e.getMessage(), e);
                            }
                        }
                    );
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
        LogMessageService logMessageService = new LogMessageServiceImpl();
        while (futureIterator.hasNext()) {
            Future<LogMessage> future = futureIterator.next();
            if (future.isDone()) {
//                logMessageService.saveToLogger(future.get());
                LogMessage logMessage = future.get();
                futureIterator.remove();
            }
            if (!futureIterator.hasNext()) {
                futureIterator = tasks.listIterator();
            }
            Thread.sleep(1);
        }
    }
}