package org.bublik.task;

import org.bublik.model.Chunk;
import org.bublik.model.LogMessage;

import java.sql.SQLException;
import java.util.concurrent.Callable;

public class Worker implements Callable<LogMessage> {
    private final Chunk<?> chunk;

    public Worker(Chunk<?> chunk) {
        this.chunk = chunk;
    }

    @Override
    public LogMessage call() throws SQLException {
        return chunk.getSourceStorage().callWorker(chunk);
    }
}
