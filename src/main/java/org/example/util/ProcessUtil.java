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
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.example.util.ColumnUtil.*;
import static org.example.util.SQLUtil.buildCopyStatement;

public class ProcessUtil {
    private static final Logger logger = LogManager.getLogger(ProcessUtil.class);

    public void initiateProcessFromDatabase(Properties fromProperties,
                                            Properties toProperties,
                                            SQLStatement sqlStatement,
                                            Integer threads) {
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
            List<Future<StringBuffer>> tasks = new ArrayList<>();
            map.forEach((key, chunk) ->
                tasks.add(
                        executorService.
                                submit(new Runner(
                                    fromProperties,
                                    toProperties,
                                    new SQLStatement(sqlStatement),
                                    chunk
                                ))
                )
            );

            // Тут реализовать COPY в пачке тредов (так COPY будет работать быстрее)
            // учесть это при отметке обработанных чанков

            // Проверить что происходит при падении COPY в плане фиксации на базе
            
            Iterator<Future<StringBuffer>> futureIterator = tasks.listIterator();
            while (futureIterator.hasNext()) {
                Future<StringBuffer> future = futureIterator.next();
                if (future.isDone()) {
                    StringBuffer stringBuffer = future.get();
                    futureIterator.remove();
                }
                if (!futureIterator.hasNext()) {
                    futureIterator = tasks.listIterator();
                }
                Thread.sleep(1);
            }

            executorService.shutdown();
            DatabaseUtil.closeConnection(connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public RunnerResult initiateProcessToDatabase(Properties toProperties, ResultSet fetchResultSet,
                                                         SQLStatement sqlStatement, Chunk chunk) {
        LogMessage logMessage = null;
        SQLException sqlException = null;
        try {
            if (fetchResultSet.next()) {
                Connection connection = DatabaseUtil.getConnection(toProperties);
                StringBuilder stringBuilder = fetchForCopy(fetchResultSet, sqlStatement, chunk);

                sqlStatement.setTargetColumns(readTargetColumnsFromDB(connection, sqlStatement));
                CopyManager copyManager = new CopyManager((BaseConnection) connection);
                String copySQL = buildCopyStatement(sqlStatement);
                long start = System.currentTimeMillis();
                InputStream inputStream = new ByteArrayInputStream(String.valueOf(stringBuilder).getBytes());
                long copyCount;
                try {
                    copyCount = copyManager.copyIn(copySQL, inputStream);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                logMessage = new LogMessage(sqlStatement.getFromTaskName(), sqlStatement.getFromTableName(), (int) copyCount,
                        chunk.startRowId(), chunk.endRowId(), chunk.chunkId());
                logger.info(" {} :\t\tCOPY {}\t {}ms", logMessage.fromTableName(), logMessage,
                        (System.currentTimeMillis() - start));

                DatabaseUtil.closeConnection(connection);
            } else {
                logMessage = new LogMessage(sqlStatement.getFromTaskName(), sqlStatement.getFromTableName(), 0,
                        chunk.startRowId(), chunk.endRowId(), chunk.chunkId());
                logger.info(" {} :\t\tFETCH {}\t", logMessage.fromTableName(), logMessage);
            }
        } catch (SQLException e) {
            logger.error("{} \t {}", sqlStatement.getFromTableName(), e);
            e.printStackTrace();
            sqlException = e;
        }
        return new RunnerResult(logMessage, sqlException);
    }

    private StringBuilder fetchForCopy(ResultSet fetchResultSet, SQLStatement sqlStatement, Chunk chunk) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        int rowCount = 0;
        long start = System.currentTimeMillis();
        do {
            for (int i = 1; i <= fetchResultSet.getMetaData().getColumnCount(); i++) {
/*
                        int columnType = fetchResultSet.getMetaData().getColumnType(i);
                        switch (columnType){
                            case 2004:
                                Blob blob = fetchResultSet.getBlob(i);
                                stringBuilder.append(blob == null ? "\\N" : blob);
                                break;
                            case 2005:
                                Clob clob = fetchResultSet.getClob(i);
                                // поправить, когда приезжает символ \\
                                String stringClob =
                                        clob.getSubString(1L, (int) clob.length())
                                                .replace("\t", "\\t")
                                                .replace("\n","\\n");
                                stringBuilder.append(stringClob.isEmpty() ? "\\N" : stringClob);
                                break;
                            default:
                                stringBuilder.append(fetchResultSet.getObject(i) == null ? "\\N" : fetchResultSet.getObject(i));
                                break;
                        }
*/
                stringBuilder.append(fetchResultSet.getObject(i) == null ? "\\N" : fetchResultSet.getObject(i));
                if (i != fetchResultSet.getMetaData().getColumnCount()) {
                    stringBuilder.append("\t");
                }
            }
            stringBuilder.append("\n");
//                    System.out.println(stringBuilder);
//                    System.out.println();
            rowCount++;
        } while (fetchResultSet.next());
        LogMessage logMessage = new LogMessage(sqlStatement.getFromTaskName(), sqlStatement.getFromTableName(), rowCount,
                chunk.startRowId(), chunk.endRowId(), chunk.chunkId());
        logger.info(" {} :\t\tFETCH {}\t {}ms", logMessage.fromTableName(), logMessage,
                (System.currentTimeMillis() - start));

        return stringBuilder;
    }

//    record FetchResult(StringBuilder stringBuilder, LogMessage logMessage) {}
}
