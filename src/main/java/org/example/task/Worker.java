package org.example.task;

import lombok.extern.slf4j.Slf4j;
import org.example.model.ChunkDeprecated;
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
    private final ChunkDeprecated chunkDeprecated;
    private final Map<String, Integer> columnsFromDB;

    private LogMessage logMessage;

    public Worker(ChunkDeprecated chunkDeprecated,
                  Map<String, Integer> columnsFromDB) {
        this.chunkDeprecated = chunkDeprecated;
        this.columnsFromDB = columnsFromDB;
    }

    @Override
    public LogMessage call() {
        var mdcToTableName = MDC.putCloseable("toTableName", chunkDeprecated.config().toTableName());
        try (Connection connection = DatabaseUtil.getConnectionDbFrom()) {
            String query = chunkDeprecated.buildFetchStatement(columnsFromDB);
//            System.out.println(query);
            ResultSet fetchResultSet = chunkDeprecated.getData(connection, query);
            RunnerResult runnerResult =
                    new CopyToPGInitiator()
                            .initiateProcessToDatabase(fetchResultSet, chunkDeprecated);
            logMessage = runnerResult.logMessage();
            fetchResultSet.close();
            if (runnerResult.logMessage() != null && runnerResult.e() == null) {
                chunkDeprecated.markChunkAsProceed(connection);
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        } finally {
            mdcToTableName.close();
        }
        return logMessage;
    }
}
