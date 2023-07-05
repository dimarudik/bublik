package org.example.task;

import lombok.AllArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.model.Chunk;
import org.example.model.LogMessage;
import org.example.model.RunnerResult;
import org.example.model.SQLStatement;
import org.example.util.DatabaseUtil;
import org.example.util.ProcessUtil;

import java.sql.*;
import java.util.Properties;

import static org.example.util.SQLUtil.buildSQLFetchStatement;

// переделать на Callable
@AllArgsConstructor
public class Runner implements Runnable{
    private Properties fromProperties;
    private Properties toProperties;
    private SQLStatement sqlStatement;
    private Chunk chunk;
    private static final Logger logger = LogManager.getLogger(Runner.class);

    @Override
    public void run() {
        try {
            Connection connection = DatabaseUtil.getConnection(fromProperties);
            PreparedStatement statement = connection.prepareStatement(buildSQLFetchStatement(sqlStatement));
            if (sqlStatement.getNumberColumn() == null) {
                statement.setString(1, chunk.getStartRowId());
                statement.setString(2, chunk.getEndRowId());
            } else {
                statement.setLong(1, chunk.getStartId());
                statement.setLong(2, chunk.getEndId());
            }
            ResultSet fetchResultSet = statement.executeQuery();
            RunnerResult runnerResult = ProcessUtil.initiateProcessToDatabase(toProperties, fetchResultSet, sqlStatement, chunk);
            fetchResultSet.close();
            statement.close();
            if (runnerResult.e() == null) {
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
    }
}
