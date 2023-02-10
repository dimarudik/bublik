package org.example.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.model.Chunk;
import org.example.model.SQLStatement;
import org.example.task.Runner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.example.util.ColumnUtil.*;
import static org.example.util.SQLUtil.buildInsertStatement;

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
            //e.printStackTrace();
        }
    }

    public static void initiateProcessToDatabase(Properties toProperties, ResultSet fetchResultSet,
                                                 SQLStatement sqlStatement, Chunk chunk) {
        try {
            if (fetchResultSet.next()) {
                Connection connection = DatabaseUtil.getConnection(toProperties);
                connection.setAutoCommit(false);
                sqlStatement.setTargetColumns(readTargetColumnsFromDB(connection, sqlStatement));
                PreparedStatement statement = connection.prepareStatement(buildInsertStatement(sqlStatement));
                long start = System.currentTimeMillis();
                int rowCount = 0;

                do {
                    for (int i = 1; i <= fetchResultSet.getMetaData().getColumnCount(); i++) {

                        if (sqlStatement.getColumn2Rule().get(i) == null) {

                            switch (fetchResultSet.getMetaData().getColumnType(i)) {
                                case 2004:
                                    if (fetchResultSet.getObject(i) != null) {
                                        statement.setBytes(i, convertBlobToBytes(fetchResultSet, i));
                                    } else {
                                        statement.setObject(i, null);
                                    }
                                    break;
                                case 93:
                                    Date localDate = fetchResultSet.getTimestamp(i);
                                    statement.setObject(i, localDate);
                                    break;
                                default:
//                                logger.info("Here... " + buildInsertStatement(sqlStatement));
//                                logger.info(fetchResultSet.getMetaData().getColumnType(i));
//                                System.out.println(statement + " " + fetchResultSet.getMetaData().getColumnType(i) + " " + fetchResultSet.getObject(i));
                                    statement.setObject(i, fetchResultSet.getObject(i));
//                                System.out.println("Here...");
                                    break;
                            }
                        } else {
                            switch (sqlStatement.getColumn2Rule().get(i).getColumnType()) {
                                case "boolean":
                                    if (fetchResultSet.getObject(i) != null) {
                                        statement.setBoolean(i, getStateByDefinition(
                                                sqlStatement.getColumn2Rule().get(i).getRuleDefinition(),
                                                fetchResultSet.getObject(i)));
                                    } else {
                                        statement.setObject(i, null);
                                    }
                                    break;
                                default:
                                    statement.setObject(i, null);
                                    break;
                            }
                        }


                    }
                    rowCount++;
//                System.out.println(statement);
                    statement.addBatch();
//                }
                } while (fetchResultSet.next());

                String logMessage = " of " + rowCount +
                        " rows from ROWID " + chunk.getStartRowId() +
                        " to ROWID " + chunk.getEndRowId() +
                        " of chunk_id " + chunk.getChunkId() +
                        " =\t";
                logger.info("\t" + sqlStatement.getFromTableName() + " :\tFETCH" + logMessage +
                        (System.currentTimeMillis() - start) + " ms");
                start = System.currentTimeMillis();
                statement.executeBatch();
                logger.info("\t" + sqlStatement.getFromTableName() + " :\t\tINSERT" + logMessage +
                        (System.currentTimeMillis() - start) + " ms");
                connection.commit();

                statement.close();
                DatabaseUtil.closeConnection(connection);
            }
        } catch (SQLException e) {
            logger.error("\t" + sqlStatement.getFromTableName() + " : " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static boolean getStateByDefinition(String ruleDefinition, Object o) {
        String[] strings = ruleDefinition.split(",");
        return !o.toString().equals(strings[0]);
    }
}
