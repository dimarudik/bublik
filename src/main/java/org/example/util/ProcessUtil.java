package org.example.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.constants.SourceContext;
import org.example.constants.SourceContextHolder;
import org.example.model.Chunk;
import org.example.model.LogMessage;
import org.example.model.PGChunk;
import org.example.model.Config;
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

public class ProcessUtil {
    private static final Logger logger = LogManager.getLogger(ProcessUtil.class);
    private SourceContextHolder contextHolder = null;

    public void initiateProcessFromDatabase(Properties fromProperties,
                                            Properties toProperties,
                                            List<Config> configs,
                                            Integer threads,
                                            Boolean initPGChunks,
                                            Boolean copyPGChunks) {
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        try {
            Connection connection = DatabaseUtil.getConnection(fromProperties);
            if (connection.getMetaData().getDriverName().split(" ")[0].equals(LABEL_ORACLE)) {
                contextHolder = new SourceContextHolder(SourceContext.Oracle);
            } else if (connection.getMetaData().getDriverName().split(" ")[0].equals(LABEL_POSTGRESQL)) {
                contextHolder = new SourceContextHolder(SourceContext.PostgreSQL);
            } else {
                logger.error("Unknown Source Database!");
                return;
            }
            Chunk chunk0 = new PGChunk(1, 1L, 2L, null);
            List<Future<LogMessage>> tasks = new ArrayList<>();
            if (contextHolder.sourceContext().toString().equals(LABEL_ORACLE)){
                Map<Integer, Chunk> chunkMap = new TreeMap<>(getStartEndRowIdMap(connection, configs));
                chunkMap.forEach((key, chunk) -> {
                            try {
                                if (tableExists(connection,
                                        chunk.config().fromSchemaName(),
                                        chunk.config().fromTableName())) {
                                    tasks.add(
                                            executorService.
                                                    submit(new Worker(
                                                            fromProperties,
                                                            toProperties,
                                                            chunk,
                                                            contextHolder,
                                                            readOraSourceColumns(connection, chunk.config())
                                                    ))
                                    );
                                }
                            } catch (SQLException e) {
                                e.printStackTrace();
                                logger.error(e.getMessage());
                            }
                        }
                );
            } else if (contextHolder.sourceContext().toString().equals(LABEL_POSTGRESQL)){
                if (initPGChunks != null && initPGChunks) {
                    fillPGChunks(connection, configs);
                } else {
                    return;
                }
                if (copyPGChunks != null && copyPGChunks) {
                    Map<Integer, Chunk> chunkMap = new TreeMap<>(getStartEndCTIDMap(connection, configs));
                    chunkMap.forEach((key, chunk) -> {
                                try {
                                    if (tableExists(connection,
                                            chunk.config().fromSchemaName(),
                                            chunk.config().fromTableName())) {
                                        tasks.add(
                                                executorService.
                                                        submit(new Worker(
                                                                fromProperties,
                                                                toProperties,
                                                                chunk,
                                                                contextHolder,
                                                                readPGSourceColumns(connection, chunk.config())
                                                        ))
                                        );
                                    }
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                    logger.error(e.getMessage());
                                }
                            }
                    );
                } else {
                    return;
                }
            } else return;
            futureProceed(tasks);

            executorService.shutdown();
            DatabaseUtil.closeConnection(connection);
        } catch (SQLException | InterruptedException | ExecutionException e) {
            executorService.shutdown();
            logger.error("Stopping all threads... {}", e.getMessage());
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