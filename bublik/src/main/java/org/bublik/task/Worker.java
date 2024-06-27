package org.bublik.task;

import org.bublik.model.Chunk;
import org.bublik.model.LogMessage;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Callable;

public class Worker implements Callable<LogMessage> {
    private final Chunk<?> chunk;
    private final Map<String, Integer> columnsFromDB;

    public Worker(Chunk<?> chunk,
                  Map<String, Integer> columnsFromDB) {
        this.chunk = chunk;
        this.columnsFromDB = columnsFromDB;
    }

    @Override
    public LogMessage call() throws SQLException {
        return chunk.getSourceStorage().callWorker(chunk, columnsFromDB);
    }
}
