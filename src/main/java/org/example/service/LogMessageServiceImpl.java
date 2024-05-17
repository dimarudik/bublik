package org.example.service;

import org.example.model.Chunk;
import org.example.model.LogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class LogMessageServiceImpl implements LogMessageService{
    private static final Logger logger = LoggerFactory.getLogger(LogMessageServiceImpl.class);

    @Override
    public void saveToLogger(Chunk<?> chunk, int recordCount, long start, String operation) {
        LogMessage logMessage = new LogMessage(
                chunk.getConfig().fromTaskName(),
                chunk.getConfig().fromTableName(),
                recordCount,
                chunk.getStart().toString(),
                chunk.getEnd().toString(),
                chunk.getId());
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
