package org.bublik.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record LogMessage (
                          int rowCount,
                          long start,
                          long stop,
                          String operation,
                          Chunk<?> chunk) {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMessage.class);

    @Override
    public String toString() {
        String toTableName = chunk.getTargetTable() == null ? "" : " to " + chunk.getTargetTable().getTableName();
        return  "from " + chunk.getSourceTable().getTableName() +
                toTableName +
                " of " + rowCount +
                " rows (start:" + chunk.getStart() +
                ", end:" + chunk.getEnd() +
                ") chunk_id:" + chunk.getId();
    }

    public void loggerChunkInfo() {
        LOGGER.info("{} {}\t {} sec",
                operation(),
                this,
                Math.round((float) (stop() - start()) / 10) / 100.0);
    }
}
