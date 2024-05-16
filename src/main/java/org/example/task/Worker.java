package org.example.task;

import lombok.extern.slf4j.Slf4j;
import org.example.model.ChunkDerpicated;
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
    private final ChunkDerpicated chunkDerpicated;
    private final Map<String, Integer> columnsFromDB;

    private LogMessage logMessage;

    public Worker(ChunkDerpicated chunkDerpicated,
                  Map<String, Integer> columnsFromDB) {
        this.chunkDerpicated = chunkDerpicated;
        this.columnsFromDB = columnsFromDB;
    }

    @Override
    public LogMessage call() {
        var mdcToTableName = MDC.putCloseable("toTableName", chunkDerpicated.config().toTableName());
        try (Connection connection = DatabaseUtil.getConnectionDbFrom()) {
            String query = chunkDerpicated.buildFetchStatement(columnsFromDB);
//            System.out.println(query);
            ResultSet fetchResultSet = chunkDerpicated.getData(connection, query);
            RunnerResult runnerResult =
                    new CopyToPGInitiator()
                            .initiateProcessToDatabase(fetchResultSet, chunkDerpicated);
            logMessage = runnerResult.logMessage();
            fetchResultSet.close();
            if (runnerResult.logMessage() != null && runnerResult.e() == null) {
                chunkDerpicated.markChunkAsProceed(connection);
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        } finally {
            mdcToTableName.close();
        }
        return logMessage;
    }
}
