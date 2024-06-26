package org.bublik.task;

import org.bublik.model.Chunk;
import org.bublik.model.LogMessage;
import org.bublik.service.ChunkService;
import org.bublik.util.CopyToPGInitiator;

import java.sql.Connection;
import java.sql.ResultSet;
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
//        Connection connection = DatabaseUtil.getPoolConnectionDbFrom();
        Connection connection = chunk.getSourceStorage().getConnection();
        String query = chunk.buildFetchStatement(columnsFromDB);
        ResultSet fetchResultSet = chunk.getData(connection, query);
        ChunkService.set(chunk);
        LogMessage logMessage;
        try {
            CopyToPGInitiator copyToPGInitiator = new CopyToPGInitiator();
            logMessage = copyToPGInitiator.start(fetchResultSet);
        } catch (SQLException | RuntimeException e) {
            fetchResultSet.close();
            connection.close();
            throw e;
        }
        fetchResultSet.close();
        chunk.markChunkAsProceed(connection);
        connection.close();
        return logMessage;
    }
}
