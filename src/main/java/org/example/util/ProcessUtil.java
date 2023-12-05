package org.example.util;

import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.model.Chunk;
import org.example.model.LogMessage;
import org.example.model.RunnerResult;
import org.example.model.SQLStatement;
import org.example.task.Worker;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.example.util.ColumnUtil.*;
import static org.example.util.SQLUtil.*;

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
//            TreeMap<Integer, Chunk> map = new TreeMap<>(getStartEndRowIdMap(connection, sqlStatement));
            Map<Integer, Chunk> chunkMap = new HashMap<>(getStartEndRowIdMap(connection, sqlStatement));
            logger.info(sqlStatement.fromTableName() + " "  +
                    chunkMap.keySet().stream().min(Integer::compareTo).orElse(0) + " " +
                    chunkMap.keySet().stream().max(Integer::compareTo).orElse(0));
            List<Future<StringBuffer>> tasks = new ArrayList<>();
            chunkMap.forEach((key, chunk) ->
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
                    logMessage = fetchAndCopy(connection, fetchResultSet, sqlStatement, chunk);
                }
                DatabaseUtil.closeConnection(connection);
            } else {
                logMessage = saveToLogger(
                        sqlStatement,
                        chunk,
                        0,
                        System.currentTimeMillis(),
                        "NO ROWS FETCH");
            }
        } catch (SQLException e) {
            logger.error("{} \t {}", sqlStatement.fromTableName(), e);
            return new RunnerResult(logMessage, e);
        }
        return new RunnerResult(logMessage, null);
    }

    private LogMessage fetchAndCopy(Connection connection,
                                    ResultSet fetchResultSet,
                                    SQLStatement sqlStatement,
                                    Chunk chunk) throws SQLException {
        int rowCount = 0;
        Map<String, String> columnsToDB = readTargetColumnsFromDB(connection, sqlStatement);
        Map<String, String> neededColumnsToDB = getNeededTargetColumnsAndTypes(sqlStatement, columnsToDB);
        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connection);
        String[] columnNames = neededColumnsToDB.keySet().toArray(new String[0]);
        SimpleRowWriter.Table table =
                new SimpleRowWriter.Table(sqlStatement.toSchemaName(), sqlStatement.toTableName(), columnNames);
        long start = System.currentTimeMillis();
        try (SimpleRowWriter writer = new SimpleRowWriter(table, pgConnection)) {
            do {
                writer.startRow((row) -> {
                    for (Map.Entry<String, String> entry : neededColumnsToDB.entrySet()) {
                        switch (entry.getValue()) {
                            case "varchar":
                                try {
                                    String s = fetchResultSet.getString(entry.getKey());
                                    row.setText(entry.getKey(), s);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "numeric":
                                try {
                                    Object o = fetchResultSet.getObject(entry.getKey());
                                    Long aLong = fetchResultSet.getLong(entry.getKey());
                                    if (o == null) {
                                        row.setDate(entry.getKey(), null);
                                        break;
                                    }
                                    row.setNumeric(entry.getKey(), aLong);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "int8":
                                try {
                                    Object o = fetchResultSet.getObject(entry.getKey());
                                    long l = fetchResultSet.getLong(entry.getKey());
                                    if (o == null) {
                                        row.setDate(entry.getKey(), null);
                                        break;
                                    }
                                    row.setLong(entry.getKey(), l);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "float8":
                                try {
                                    Object o = fetchResultSet.getObject(entry.getKey());
                                    float aFloat = fetchResultSet.getFloat(entry.getKey());
                                    if (o == null) {
                                        row.setDate(entry.getKey(), null);
                                        break;
                                    }
                                    row.setDouble(entry.getKey(), Double.valueOf(aFloat));
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "timestamp":
                                try {
                                    Timestamp timestamp = fetchResultSet.getTimestamp(entry.getKey());
                                    if (timestamp == null) {
                                        row.setDate(entry.getKey(), null);
                                        break;
                                    }
                                    long l = timestamp.getTime();
                                    LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(l),
                                            TimeZone.getDefault().toZoneId());
                                    row.setTimeStamp(entry.getKey(), localDateTime);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "date":
                                try {
                                    Timestamp timestamp = fetchResultSet.getTimestamp(entry.getKey());
                                    if (timestamp == null) {
                                        row.setDate(entry.getKey(), null);
                                        break;
                                    }
                                    long l = timestamp.getTime();
                                    LocalDate localDate = Instant.ofEpochMilli(l)
                                            .atZone(ZoneId.systemDefault()).toLocalDate();
                                    row.setDate(entry.getKey(), localDate);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            default:
                                break;
                        }
                    }
                });
                rowCount++;
            } while (fetchResultSet.next());
        }
/*
        StringBuilder stringBuilder = new StringBuilder();
        do {
            for (int i = 1; i <= fetchResultSet.getMetaData().getColumnCount(); i++) {
                stringBuilder.append(fetchResultSet.getObject(i) == null ? "\\N" : fetchResultSet.getObject(i));
                if (i != fetchResultSet.getMetaData().getColumnCount()) {
                    stringBuilder.append("\t");
                }
            }
            stringBuilder.append("\n");
            rowCount++;
        } while (fetchResultSet.next());
*/
        return saveToLogger(sqlStatement, chunk, rowCount, start, "COPY");
    }

    private LogMessage copyToTarget(Connection connection,
                                    StringBuilder stringBuilder,
                                    SQLStatement sqlStatement,
                                    Chunk chunk) {
        Map<String, String> columnsToDB = readTargetColumnsFromDB(connection, sqlStatement);
        String copySQL = buildCopyStatement(sqlStatement, columnsToDB);
        CopyManager copyManager = null;
        try {
            copyManager = new CopyManager((BaseConnection) connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long start = System.currentTimeMillis();
        InputStream inputStream = new ByteArrayInputStream(String.valueOf(stringBuilder).getBytes());
        long copyCount = 0;
        try {
            connection.setAutoCommit(false);
            copyCount = copyManager.copyIn(copySQL, inputStream);
            connection.commit();
        } catch (IOException | SQLException e) {
            logger.error("{} \t {}", sqlStatement.fromTableName(), e);
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
        logger.info(" {} :\t\t{} {}\t {} sec",
                logMessage.fromTableName(),
                operation,
                logMessage,
                Math.round( (float) (System.currentTimeMillis() - start) / 10) / 100.0);
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
