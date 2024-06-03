package org.example.task;

import org.example.model.Chunk;
import org.example.model.RunnerResult;
import org.example.service.ChunkService;
import org.example.util.CopyToPGInitiator;
import org.example.util.DatabaseUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Callable;

public class Worker implements Callable<RunnerResult> {
    private final Chunk<?> chunk;
    private final Map<String, Integer> columnsFromDB;

    public Worker(Chunk<?> chunk,
                  Map<String, Integer> columnsFromDB) {
        this.chunk = chunk;
        this.columnsFromDB = columnsFromDB;
    }

    @Override
    public RunnerResult call() throws SQLException {
        Connection connection = DatabaseUtil.getConnectionDbFrom();
        String query = chunk.buildFetchStatement(columnsFromDB);
        ResultSet fetchResultSet = chunk.getData(connection, query);
        ChunkService.set(chunk);
        RunnerResult runnerResult =
                new CopyToPGInitiator()
                        .initiateProcessToDatabase(fetchResultSet);
        fetchResultSet.close();
        if (runnerResult.logMessage() != null && runnerResult.e() == null) {
            chunk.markChunkAsProceed(connection);
        }
        connection.close();
        return runnerResult;
    }
}
