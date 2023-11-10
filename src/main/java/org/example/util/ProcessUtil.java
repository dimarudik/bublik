package org.example.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.model.Chunk;
import org.example.model.LogMessage;
import org.example.model.RunnerResult;
import org.example.model.SQLStatement;
import org.example.task.Runner;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.example.util.ColumnUtil.*;
import static org.example.util.SQLUtil.buildCopyStatement;

public class ProcessUtil {
    private static final Logger logger = LogManager.getLogger(ProcessUtil.class);

    public static void initiateProcessFromDatabase(Properties fromProperties,
                                                   Properties toProperties,
                                                   SQLStatement sqlStatement,
                                                   int threads) {
        // Надо переделать на Callable
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        try {
            Connection connection = DatabaseUtil.getConnection(fromProperties);
//            !!!!!!!!!!!!!!!!!!!!!!!!!!
//            connection.unwrap(oracle.jdbc.OracleConnection.class).setSchema(sqlStatement.getFromSchemaName());
//            !!!!!!!!!!!!!!!!!!!!!!!!!!
            sqlStatement.setSourceColumns(readSourceColumnsFromDB(connection, sqlStatement));
            sqlStatement.setColumn2Rule(getColumn2RuleMap(sqlStatement));
            TreeMap<Integer, Chunk> map = new TreeMap<>(getStartEndRowIdMap(connection, sqlStatement));
            logger.info(sqlStatement.getFromTableName() + " "  +
                    map.keySet().stream().min(Integer::compareTo).orElse(0) + " " +
                    map.keySet().stream().max(Integer::compareTo).orElse(0));
            // тут должен быть submit
            map.forEach((key, chunk) ->
                executorService.execute(new Runner(
                        fromProperties,
                        toProperties,
                        sqlStatement,
                        chunk)));
            executorService.shutdown();
            DatabaseUtil.closeConnection(connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    public static RunnerResult initiateProcessToDatabase(Properties toProperties, ResultSet fetchResultSet,
                                                         SQLStatement sqlStatement, Chunk chunk) {
        LogMessage logMessage = null;
        SQLException sqlException = null;
        SQLStatement threadSafeStatement = new SQLStatement(sqlStatement);
        try {
            if (fetchResultSet.next()) {
                Connection connection = DatabaseUtil.getConnection(toProperties);
                CopyManager copyManager = new CopyManager((BaseConnection) connection);
                threadSafeStatement.setTargetColumns(readTargetColumnsFromDB(connection, threadSafeStatement));
                String copySQL = buildCopyStatement(threadSafeStatement);
                long start = System.currentTimeMillis();
                int rowCount = 0;
                StringBuffer stringBuffer = new StringBuffer();
                do {
                    for (int i = 1; i <= fetchResultSet.getMetaData().getColumnCount(); i++) {
                        stringBuffer.append(fetchResultSet.getObject(i) == null ? "\\N" : fetchResultSet.getObject(i));
                        if (i != fetchResultSet.getMetaData().getColumnCount()) {
                            stringBuffer.append("\t");
                        }
                    }
                    stringBuffer.append("\n");
                    rowCount++;
                } while (fetchResultSet.next());

                logMessage = new LogMessage(threadSafeStatement.getFromTaskName(), threadSafeStatement.getFromTableName(), rowCount,
                        chunk.getStartRowId(), chunk.getEndRowId(), chunk.getChunkId());

                logger.info(" {} :\t\tFETCH {}\t {}ms", logMessage.fromTableName(), logMessage,
                        (System.currentTimeMillis() - start));

                start = System.currentTimeMillis();
                InputStream inputStream = new ByteArrayInputStream(String.valueOf(stringBuffer).getBytes());
                long copyCount;
                try {
                    copyCount = copyManager.copyIn(copySQL, inputStream);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                logMessage = new LogMessage(threadSafeStatement.getFromTaskName(), threadSafeStatement.getFromTableName(), (int) copyCount,
                        chunk.getStartRowId(), chunk.getEndRowId(), chunk.getChunkId());
                logger.info(" {} :\t\tCOPY {}\t {}ms", logMessage.fromTableName(), logMessage,
                        (System.currentTimeMillis() - start));

                DatabaseUtil.closeConnection(connection);
            } else {
                logMessage = new LogMessage(threadSafeStatement.getFromTaskName(), threadSafeStatement.getFromTableName(), 0,
                        chunk.getStartRowId(), chunk.getEndRowId(), chunk.getChunkId());
                logger.info(" {} :\t\tFETCH {}\t", logMessage.fromTableName(), logMessage);
            }
        } catch (SQLException e) {
            logger.error("{} \t {}", threadSafeStatement.getFromTableName(), e);
            e.printStackTrace();
            sqlException = e;
        }
        return new RunnerResult(logMessage, sqlException);
    }

    private static boolean getStateByDefinition(String ruleDefinition, Object o) {
        String[] strings = ruleDefinition.split(",");
        return !o.toString().equals(strings[0]);
    }
}
