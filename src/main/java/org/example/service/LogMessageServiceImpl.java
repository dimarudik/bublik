package org.example.service;

import org.example.model.ChunkDerpicated;
import org.example.model.LogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class LogMessageServiceImpl implements LogMessageService{
    private static final Logger logger = LoggerFactory.getLogger(LogMessageServiceImpl.class);

    @Override
    public void saveToLogger(ChunkDerpicated chunkDerpicated, int recordCount, long start, String operation) {
        LogMessage logMessage = new LogMessage(
                chunkDerpicated.config().fromTaskName(),
                chunkDerpicated.config().fromTableName(),
                recordCount,
                chunkDerpicated.startRowId(),
                chunkDerpicated.endRowId(),
                chunkDerpicated.chunkId());
//        Instant instant = Instant.now().getEpochSecond();
        logger.info(" {} :\t\t{} {}\t {} sec",
                logMessage.fromTableName(),
                operation,
                logMessage,
//                Math.round( (float) (System.currentTimeMillis() - start) / 10) / 100.0
                Math.round( (float) (Instant.now().getEpochSecond() - start) / 10) / 100.0
        );
    }
}
