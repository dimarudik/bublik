package org.example.util;

import lombok.extern.slf4j.Slf4j;
import org.example.constants.SourceContext;
import org.example.constants.SourceContextHolder;
import org.example.model.Chunk;
import org.example.model.Config;
import org.example.model.LogMessage;
import org.example.service.LogMessageService;
import org.example.service.LogMessageServiceImpl;
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
import static org.example.util.TableUtil.tableExists;

@Slf4j
public class ProcessUtil {
    private SourceContextHolder contextHolder = null;

    public void initiateProcessFromDatabase(List<Config> configs,
                                            Integer threads,
                                            Boolean initPGChunks,
                                            Boolean copyPGChunks) {
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        try (Connection connection = DatabaseUtil.getConnectionDbFrom()) {
            if (connection.getMetaData().getDriverName().split(" ")[0].equals(LABEL_ORACLE)) {
                contextHolder = new SourceContextHolder(SourceContext.Oracle);
            } else if (connection.getMetaData().getDriverName().split(" ")[0].equals(LABEL_POSTGRESQL)) {
                contextHolder = new SourceContextHolder(SourceContext.PostgreSQL);
            } else {
                log.error("Unknown Source Database!");
                return;
            }
            List<Future<LogMessage>> tasks = new ArrayList<>();
            if (contextHolder.sourceContext().toString().equals(LABEL_ORACLE)){
                Map<Integer, Chunk> chunkMap = new TreeMap<>(getStartEndRowIdMap(connection, configs));
                chunkMap.forEach((key, chunk) -> {
                        try {
                            if (tableExists(connection,
                                    chunk.config().fromSchemaName(),
                                    chunk.config().fromTableName())) {
                                Map<String, Integer> orderedColumns = new HashMap<>();
                                chunk.config().columnToColumn().forEach((k, v) -> orderedColumns.put(k, null));
//                                System.out.println(orderedColumns);
                                tasks.add(
                                        executorService.
                                                submit(new Worker(
                                                        chunk,
                                                        orderedColumns
                                                ))
                                );
                            }
                        } catch (SQLException e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                );
            } else if (contextHolder.sourceContext().toString().equals(LABEL_POSTGRESQL)){
                if (initPGChunks) {
                    fillPGChunks(connection, configs);
                }
                if (copyPGChunks) {
                    Map<Integer, Chunk> chunkMap = new TreeMap<>(getStartEndCTIDMap(connection, configs));
                    chunkMap.forEach((key, chunk) -> {
                            try {
                                if (tableExists(connection,
                                        chunk.config().fromSchemaName(),
                                        chunk.config().fromTableName())) {
                                    Map<String, Integer> orderedColumns = new HashMap<>();
                                    chunk.config().columnToColumn().forEach((k, v) -> orderedColumns.put(k, null));
                                    tasks.add(
                                            executorService.submit(new Worker(chunk, orderedColumns))
                                    );
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