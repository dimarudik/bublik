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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import static org.example.util.SQLUtil.buildSQLFetchStatement;

public class Worker implements Callable<StringBuffer> {
    private final Properties fromProperties;
    private final Properties toProperties;
    private final SQLStatement sqlStatement;
    private final Chunk chunk;
    private final Map<String, Integer> columnsFromDB;
    private static final Logger logger = LogManager.getLogger(Worker.class);

    public Worker(Properties fromProperties,
                  Properties toProperties,
                  SQLStatement sqlStatement,
                  Chunk chunk,
                  Map<String, Integer> columnsFromDB) {
        this.fromProperties = fromProperties;
        this.toProperties = toProperties;
        this.sqlStatement = sqlStatement;
        this.chunk = chunk;
        this.columnsFromDB = columnsFromDB;
    }

    @Override
    public StringBuffer call() {
        try {
            Connection connOracle = DatabaseUtil.getConnection(fromProperties);
            String query = buildSQLFetchStatement(sqlStatement, columnsFromDB);
//            System.out.println(query);
            PreparedStatement statement = connOracle.prepareStatement(query);
            ResultSet fetchResultSet = fetchResultSetFromDB(statement);
            RunnerResult runnerResult =
                    new ProcessUtil().initiateProcessToDatabase(toProperties, fetchResultSet, sqlStatement, chunk);
            fetchResultSet.close();
            statement.close();
            if (runnerResult.logMessage() != null && runnerResult.e() == null) {
                markChunkAsProceed(connOracle);
            }
            DatabaseUtil.closeConnection(connOracle);
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        return new StringBuffer();
    }

    private ResultSet fetchResultSetFromDB(PreparedStatement statement) throws SQLException {
        if (sqlStatement.numberColumn() == null) {
            statement.setString(1, chunk.startRowId());
            statement.setString(2, chunk.endRowId());
        } else {
            statement.setLong(1, chunk.startId());
            statement.setLong(2, chunk.endId());
        }
        return statement.executeQuery();
    }

    private void markChunkAsProceed(Connection connection) throws SQLException {
        CallableStatement callableStatement =
                connection.prepareCall("CALL DBMS_PARALLEL_EXECUTE.SET_CHUNK_STATUS(?,?,2)");
        callableStatement.setString(1, sqlStatement.fromTaskName());
        callableStatement.setInt(2, chunk.chunkId());
        callableStatement.execute();
        callableStatement.close();
    }
}
