package org.example.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.model.*;
import org.example.util.CopyToPGInitiator;
import org.example.util.DatabaseUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

public class Worker implements Callable<LogMessage> {
    private final Properties fromProperties;
    private final Properties toProperties;
    private final Chunk chunk;
    private final Map<String, Integer> columnsFromDB;

    private LogMessage logMessage;
    private static final Logger logger = LogManager.getLogger(Worker.class);

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
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return logMessage;
    }
}
