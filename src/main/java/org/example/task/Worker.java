package org.example.task;

import lombok.extern.slf4j.Slf4j;
import org.example.model.Chunk;
import org.example.model.LogMessage;
import org.example.model.RunnerResult;
import org.example.util.CopyToPGInitiator;
import org.example.util.DatabaseUtil;
import org.slf4j.MDC;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Callable;

@Slf4j
public class Worker implements Callable<LogMessage> {
    private final Chunk<?> chunk;
    private final Map<String, Integer> columnsFromDB;

    private LogMessage logMessage;

    public Worker(Chunk<?> chunk,
                  Map<String, Integer> columnsFromDB) {
        this.chunk = chunk;
        this.columnsFromDB = columnsFromDB;
    }

    @Override
    public LogMessage call() {
        try (var mdcToTableName = MDC.putCloseable("toTableName", chunk.getConfig().toTableName());
             Connection connection = DatabaseUtil.getConnectionDbFrom()) {
            String query = chunk.buildFetchStatement(columnsFromDB);
            ResultSet fetchResultSet = chunk.getData(connection, query);
            RunnerResult runnerResult =
                    new CopyToPGInitiator()
                            .initiateProcessToDatabase(fetchResultSet, chunk);
            logMessage = runnerResult.logMessage();
            fetchResultSet.close();
            if (runnerResult.logMessage() != null && runnerResult.e() == null) {
                chunk.markChunkAsProceed(connection);
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return logMessage;
    }
}
