package org.example.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.model.Chunk;
import org.example.model.LogMessage;
import org.example.model.RunnerResult;
import org.example.model.SQLStatement;
import org.example.task.Worker;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.example.util.ColumnUtil.*;
import static org.example.util.SQLUtil.buildCopyStatement;
import static org.example.util.SQLUtil.buildInsertStatement;

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
            Map<String, Integer> columnsFromDB = readSourceColumnsFromDB(connection,sqlStatement);
//            sqlStatement.setColumn2Rule(getColumn2RuleMap(sqlStatement));
            TreeMap<Integer, Chunk> map = new TreeMap<>(getStartEndRowIdMap(connection, sqlStatement));
            logger.info(sqlStatement.fromTableName() + " "  +
                    map.keySet().stream().min(Integer::compareTo).orElse(0) + " " +
                    map.keySet().stream().max(Integer::compareTo).orElse(0));
            List<Future<StringBuffer>> tasks = new ArrayList<>();
            map.forEach((key, chunk) ->
                tasks.add(
                        executorService.
                                submit(new Worker(
                                        fromProperties,
                                        toProperties,
                                        sqlStatement,
                                        chunk,
                                        columnsFromDB
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
        } catch (SQLException | ExecutionException | InterruptedException e) {
            logger.error(e.getMessage());
        }
    }

    public RunnerResult initiateProcessToDatabase(Properties toProperties,
                                                  ResultSet fetchResultSet,
                                                  SQLStatement sqlStatement,
                                                  Chunk chunk) {
        LogMessage logMessage = null;
        try {
            if (fetchResultSet.next()) {
                Connection connection = DatabaseUtil.getConnection(toProperties);
                if (hasLOB(fetchResultSet)) {
                    logMessage = batchInsertToTarget(connection, fetchResultSet, sqlStatement, chunk);
                } else {
                    StringBuilder stringBuilder = fetchForCopy(fetchResultSet, sqlStatement, chunk);
                    logMessage = copyToTarget(connection, stringBuilder, sqlStatement, chunk);
                }
                DatabaseUtil.closeConnection(connection);
            } else {
                logMessage = new LogMessage(sqlStatement.fromTaskName(), sqlStatement.fromTableName(), 0,
                        chunk.startRowId(), chunk.endRowId(), chunk.chunkId());
                logger.info(" {} :\t\tFETCH {}\t", logMessage.fromTableName(), logMessage);
            }
        } catch (SQLException e) {
            logger.error("{} \t {}", sqlStatement.fromTableName(), e);
            e.printStackTrace();
            return new RunnerResult(logMessage, e);
        }
        return new RunnerResult(logMessage, null);
    }

    private StringBuilder fetchForCopy(ResultSet fetchResultSet,
                                       SQLStatement sqlStatement,
                                       Chunk chunk) throws SQLException {
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
            rowCount++;
        } while (fetchResultSet.next());
        saveToLogger(sqlStatement, chunk, rowCount, start, "FETCH");
        return stringBuilder;
    }

    private LogMessage copyToTarget(Connection connection,
                                    StringBuilder stringBuilder,
                                    SQLStatement sqlStatement,
                                    Chunk chunk) throws SQLException {
        Map<String, String> columnsToDB = readTargetColumnsFromDB(connection, sqlStatement);
        String copySQL = buildCopyStatement(sqlStatement, columnsToDB);
        CopyManager copyManager = new CopyManager((BaseConnection) connection);
        long start = System.currentTimeMillis();
        InputStream inputStream = new ByteArrayInputStream(String.valueOf(stringBuilder).getBytes());
        long copyCount;
        try {
            copyCount = copyManager.copyIn(copySQL, inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return saveToLogger(sqlStatement, chunk, (int) copyCount, start, "COPY");
    }

    private LogMessage batchInsertToTarget(Connection connection,
                                           ResultSet fetchResultSet,
                                           SQLStatement sqlStatement,
                                           Chunk chunk) throws SQLException {
        connection.setAutoCommit(false);
        Map<String, String> columnsToDB = readTargetColumnsFromDB(connection, sqlStatement);
        String insertSQL = buildInsertStatement(sqlStatement, columnsToDB);
        PreparedStatement statement = connection.prepareStatement(insertSQL);
        long start = System.currentTimeMillis();
        int fetchCount = 0;
        do {
            for (int i = 1; i <= fetchResultSet.getMetaData().getColumnCount(); i++) {
                switch (fetchResultSet.getMetaData().getColumnType(i)) {
                    case 2004:
                        if (fetchResultSet.getObject(i) != null) {
                            statement.setBytes(i, convertBlobToBytes(fetchResultSet, i));
                        } else {
                            statement.setObject(i, null);
                        }
                        break;
                    case 2005:
                        if (fetchResultSet.getObject(i) != null) {
                            statement.setBytes(i, convertClobToBytes(fetchResultSet, i).getBytes());
                        } else {
                            statement.setObject(i, null);
                        }
                        break;
                    case 93:
                        Date localDate = fetchResultSet.getTimestamp(i);
                        statement.setObject(i, localDate);
                        break;
                    default:
                        statement.setObject(i, fetchResultSet.getObject(i));
                        break;
                }
            }
            fetchCount++;
            statement.addBatch();
        } while (fetchResultSet.next());
        saveToLogger(sqlStatement, chunk, fetchCount, start, "FETCH");

        start = System.currentTimeMillis();
        statement.executeBatch();
        connection.commit();

        statement.close();
        return saveToLogger(sqlStatement, chunk, fetchCount, start, "INSERT");
    }

    public LogMessage saveToLogger(SQLStatement sqlStatement,
                                   Chunk chunk,
                                   int recordCount,
                                   long start,
                                   String operation) {
        LogMessage logMessage = new LogMessage(
                sqlStatement.fromTaskName(),
                sqlStatement.fromTableName(),
                recordCount,
                chunk.startRowId(),
                chunk.endRowId(),
                chunk.chunkId());
        logger.info(" {} :\t\t{} {}\t {}ms",
                logMessage.fromTableName(),
                operation,
                logMessage,
                (System.currentTimeMillis() - start));
        return logMessage;
    }

    private Boolean hasLOB(ResultSet fetchResultSet) throws SQLException {
        for (int i = 1; i <= fetchResultSet.getMetaData().getColumnCount(); i++) {
            int columnType = fetchResultSet.getMetaData().getColumnType(i);
            if (columnType == 2004 || columnType == 2005) {
                return true;
            }
        }
        return false;
    }
}
