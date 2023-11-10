package org.example.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.model.Chunk;
import org.example.model.RunnerResult;
import org.example.model.SQLStatement;
import org.example.util.DatabaseUtil;
import org.example.util.ProcessUtil;
import org.postgresql.PGConnection;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.Callable;

import static org.example.util.SQLUtil.buildSQLFetchStatement;

public class Runner implements Callable<StringBuffer> {
    private final Properties fromProperties;
    private final Properties toProperties;
    private final SQLStatement sqlStatement;
    private final Chunk chunk;
    private final Integer threads;
    private static final Logger logger = LogManager.getLogger(Runner.class);

    public Runner(Properties fromProperties, Properties toProperties,
                  SQLStatement sqlStatement, Chunk chunk, Integer threads) {
        this.fromProperties = fromProperties;
        this.toProperties = toProperties;
        this.sqlStatement = sqlStatement;
        this.chunk = chunk;
        this.threads = threads;
    }

    @Override
    public StringBuffer call() {
        try {
            Connection connection = DatabaseUtil.getConnection(fromProperties);
//            PGConnection pgConnection = connection.unwrap(PGConnection.class);
            String query = buildSQLFetchStatement(sqlStatement);
            PreparedStatement statement = connection.prepareStatement(query);
            if (sqlStatement.getNumberColumn() == null) {
                statement.setString(1, chunk.getStartRowId());
                statement.setString(2, chunk.getEndRowId());
            } else {
                statement.setLong(1, chunk.getStartId());
                statement.setLong(2, chunk.getEndId());
            }
            ResultSet fetchResultSet = statement.executeQuery();
            RunnerResult runnerResult =
                    ProcessUtil.initiateProcessToDatabase(toProperties, fetchResultSet, sqlStatement, chunk, threads);
            fetchResultSet.close();
            statement.close();
            if (runnerResult.logMessage() != null && runnerResult.e() == null) {
                CallableStatement callableStatement =
                        connection.prepareCall("CALL DBMS_PARALLEL_EXECUTE.SET_CHUNK_STATUS(?,?,2)");
                callableStatement.setString(1, runnerResult.logMessage().fromTaskName());
                callableStatement.setInt(2, runnerResult.logMessage().chunkId());
                callableStatement.execute();
            }
            DatabaseUtil.closeConnection(connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            //e.printStackTrace();
        }
        //logger.info("FINISHED...");
        return new StringBuffer();
    }
}
