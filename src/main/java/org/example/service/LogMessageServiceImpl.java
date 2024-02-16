package org.example.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.model.Chunk;
import org.example.model.LogMessage;

import java.time.Instant;

public class LogMessageServiceImpl implements LogMessageService{
    private static final Logger logger = LogManager.getLogger(LogMessageServiceImpl.class);

    @Override
    public void saveToLogger(Chunk chunk, int recordCount, long start, String operation) {
        LogMessage logMessage = new LogMessage(
                chunk.config().fromTaskName(),
                chunk.config().fromTableName(),
                recordCount,
                chunk.startRowId(),
                chunk.endRowId(),
                chunk.chunkId());
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
