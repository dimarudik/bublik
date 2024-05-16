package org.example.service;

import org.example.model.ChunkDeprecated;
import org.example.model.LogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class LogMessageServiceImpl implements LogMessageService{
    private static final Logger logger = LoggerFactory.getLogger(LogMessageServiceImpl.class);

    @Override
    public void saveToLogger(ChunkDeprecated chunkDeprecated, int recordCount, long start, String operation) {
        LogMessage logMessage = new LogMessage(
                chunkDeprecated.config().fromTaskName(),
                chunkDeprecated.config().fromTableName(),
                recordCount,
                chunkDeprecated.startRowId(),
                chunkDeprecated.endRowId(),
                chunkDeprecated.chunkId());
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
