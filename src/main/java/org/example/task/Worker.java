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
import java.util.Properties;
import java.util.concurrent.Callable;

@Slf4j
public class Worker implements Callable<LogMessage> {
    private final Properties fromProperties;
    private final Properties toProperties;
    private final Chunk chunk;
    private final Map<String, Integer> columnsFromDB;

    private LogMessage logMessage;
//    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    public Worker(Properties fromProperties,
                  Properties toProperties,
                  Chunk chunk,
                  Map<String, Integer> columnsFromDB) {
        this.fromProperties = fromProperties;
        this.toProperties = toProperties;
        this.chunk = chunk;
        this.columnsFromDB = columnsFromDB;
    }

    @Override
    public LogMessage call() {
        MDC.MDCCloseable mdcToTableName = MDC.putCloseable("toTableName", chunk.config().toTableName());
        try {
            Connection connection = DatabaseUtil.getConnection(fromProperties);
            String query = chunk.buildFetchStatement(columnsFromDB);
//            System.out.println(query);
            ResultSet fetchResultSet = chunk.getData(connection, query);
            RunnerResult runnerResult =
                    new CopyToPGInitiator()
                            .initiateProcessToDatabase(toProperties, fetchResultSet, chunk);
            logMessage = runnerResult.logMessage();
            fetchResultSet.close();
            if (runnerResult.logMessage() != null && runnerResult.e() == null) {
                chunk.markChunkAsProceed(connection);
            }
            DatabaseUtil.closeConnection(connection);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        finally {
            mdcToTableName.close();
        }
        return logMessage;
    }
}
