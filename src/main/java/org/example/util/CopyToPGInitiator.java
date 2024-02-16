package org.example.util;

import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.constants.SourceContextHolder;
import org.example.model.*;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;

import static org.example.constants.SQLConstants.LABEL_ORACLE;
import static org.example.constants.SQLConstants.LABEL_POSTGRESQL;
import static org.example.util.ColumnUtil.*;
import static org.example.util.ColumnUtil.convertBlobToBytes;
import static org.example.util.SQLUtil.getNeededTargetColumnsAndTypes;
import static org.example.util.TableUtil.tableExists;

public class CopyToPGInitiator {
    private static final Logger logger = LogManager.getLogger(CopyToPGInitiator.class);

    public RunnerResult initiateProcessToDatabase(Properties toProperties,
                                                  ResultSet fetchResultSet,
                                                  Chunk chunk,
                                                  SourceContextHolder contextHolder) {
        LogMessage logMessage = null;
        try {
            if (fetchResultSet.next()) {
                Connection connection = DatabaseUtil.getConnection(toProperties);
                if (tableExists(connection, chunk.config().toSchemaName(), chunk.config().toTableName())) {
                    logMessage = fetchAndCopy(connection, fetchResultSet, chunk.config(), chunk, contextHolder);
                }
                DatabaseUtil.closeConnection(connection);
            } else {
                logMessage = saveToLogger(
                        chunk,
                        0,
                        System.currentTimeMillis(),
                        "NO ROWS FETCH");
            }
        } catch (SQLException e) {
            logger.error("{} \t {}", chunk.config().fromTableName(), e);
            e.printStackTrace();
            return new RunnerResult(logMessage, e);
        }
        return new RunnerResult(logMessage, null);
    }

    private LogMessage fetchAndCopy(Connection connection,
                                    ResultSet fetchResultSet,
                                    Config config,
                                    Chunk chunk,
                                    SourceContextHolder contextHolder) {
        int rowCount = 0;
        Map<String, String> columnsToDB = readPGTargetColumns(connection, config);
        Map<String, String> neededColumnsToDB = getNeededTargetColumnsAndTypes(config, columnsToDB);
        long start = System.currentTimeMillis();
        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connection);
//        System.out.println("Create to destination connection time : " + (System.currentTimeMillis() - start) / 1000d);
        String[] columnNames = neededColumnsToDB.keySet().toArray(new String[0]);
        SimpleRowWriter.Table table =
                new SimpleRowWriter.Table(config.toSchemaName(), config.toTableName(), columnNames);
//        Arrays.asList(columnNames).forEach(k -> System.out.print(k + " "));
//        System.out.println();
        start = System.currentTimeMillis();
//        start = Instant.now().getEpochSecond();
        try (SimpleRowWriter writer = new SimpleRowWriter(table, pgConnection)) {
            do {
                writer.startRow((row) -> {
                    for (Map.Entry<String, String> entry : neededColumnsToDB.entrySet()) {
/*
                        try {
                            System.out.println(entry.getKey() + " : " + entry.getValue() + " : " + fetchResultSet.getString(entry.getKey()));
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
*/
                        switch (entry.getValue()) {
                            case "varchar":
                                try {
                                    String s = fetchResultSet.getString(entry.getKey());
                                    row.setText(entry.getKey(), s);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "bpchar":
                                try {
                                    String s = fetchResultSet.getString(entry.getKey());
                                    row.setText(entry.getKey(), s);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "text":
                                try {
                                    Object o = fetchResultSet.getObject(entry.getKey());
                                    if (o == null) {
                                        row.setText(entry.getKey(), null);
                                        break;
                                    }
                                    String s;
                                    int cIndex = getColumnIndexByColumnName(fetchResultSet, entry.getKey());
                                    if (cIndex != 0 && fetchResultSet.getMetaData().getColumnType(cIndex) == 2005) {
                                        s = convertClobToString(fetchResultSet, entry.getKey());
                                    } else {
                                        s = fetchResultSet.getString(entry.getKey());
                                    }
                                    row.setText(entry.getKey(), s);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "bigint":
                                try {
                                    Object o = fetchResultSet.getObject(entry.getKey());
                                    if (o == null) {
                                        row.setLong(entry.getKey(), null);
                                        break;
                                    }
                                    long l = fetchResultSet.getLong(entry.getKey());
                                    row.setLong(entry.getKey(), l);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "numeric":
                                try {
                                    Object o = fetchResultSet.getObject(entry.getKey());
                                    if (o == null) {
                                        row.setNumeric(entry.getKey(), null);
                                        break;
                                    }
                                    row.setNumeric(entry.getKey(), (Number) o);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "int2":
                                try {
                                    Object o = fetchResultSet.getObject(entry.getKey());
                                    if (o == null) {
                                        row.setShort(entry.getKey(), null);
                                        break;
                                    }
                                    Short aShort = fetchResultSet.getShort(entry.getKey());
                                    row.setShort(entry.getKey(), aShort);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "int4":
                                try {
                                    Object o = fetchResultSet.getObject(entry.getKey());
                                    if (o == null) {
                                        row.setInteger(entry.getKey(), null);
                                        break;
                                    }
                                    int i = fetchResultSet.getInt(entry.getKey());
                                    row.setInteger(entry.getKey(), i);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "int8":
                                try {
                                    Object o = fetchResultSet.getObject(entry.getKey());
                                    if (o == null) {
                                        row.setLong(entry.getKey(), null);
                                        break;
                                    }
                                    long l = fetchResultSet.getLong(entry.getKey());
                                    row.setLong(entry.getKey(), l);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "float8":
                                try {
                                    Object o = fetchResultSet.getObject(entry.getKey());
                                    if (o == null) {
                                        row.setDouble(entry.getKey(), null);
                                        break;
                                    }
                                    Double aDouble = fetchResultSet.getDouble(entry.getKey());
                                    row.setDouble(entry.getKey(), aDouble);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "timestamp":
                                try {
                                    Timestamp timestamp = fetchResultSet.getTimestamp(entry.getKey());
                                    if (timestamp == null) {
                                        row.setTimeStamp(entry.getKey(), null);
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
                            case "timestamptz":
                                try {
                                    Timestamp timestamp = fetchResultSet.getTimestamp(entry.getKey());
                                    if (timestamp == null) {
                                        row.setTimeStamp(entry.getKey(), null);
                                        break;
                                    }
                                    ZonedDateTime zonedDateTime =
                                            ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp.getTime()),
                                                    ZoneOffset.UTC);
                                    row.setTimeStampTz(entry.getKey(), zonedDateTime);
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
                            case "bytea":
                                try {
                                    Object o = fetchResultSet.getObject(entry.getKey());
                                    if (o == null) {
                                        row.setByteArray(entry.getKey(), null);
                                        break;
                                    }
                                    byte[] bytes = new byte[0];
                                    if (contextHolder.sourceContext().toString().equals(LABEL_ORACLE)) {
                                        bytes = convertBlobToBytes(fetchResultSet, entry.getKey());
                                    } else if (contextHolder.sourceContext().toString().equals(LABEL_POSTGRESQL)) {
                                        bytes = fetchResultSet.getBytes(entry.getKey());
                                    }
                                    row.setByteArray(entry.getKey(), bytes);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "bool":
                                try {
                                    Object o = fetchResultSet.getObject(entry.getKey());
                                    if (o == null) {
                                        row.setBoolean(entry.getKey(), null);
                                        break;
                                    }
                                    boolean b = fetchResultSet.getBoolean(entry.getKey());
                                    row.setBoolean(entry.getKey(), b);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            case "uuid":
                                try {
                                    Object o = fetchResultSet.getObject(entry.getKey());
                                    if (o == null) {
                                        row.setUUID(entry.getKey(), null);
                                        break;
                                    }
                                    UUID uuid = (UUID) o;
                                    row.setUUID(entry.getKey(), uuid);
                                    break;
                                } catch (SQLException e) {
                                    logger.error("{} {}", entry.getKey(), e);
                                }
                            default:
                                throw new RuntimeException("There is no handler for type : " + entry.getValue());
                        }
                    }
                });
                rowCount++;
            } while (fetchResultSet.next());
        } catch (SQLException | RuntimeException e) {
            e.printStackTrace();
            logger.error(e);
            return null;
        }
        return saveToLogger(chunk, rowCount, start, "COPY");
/*
        return new LogMessage(
                chunk.config().fromTaskName(),
                chunk.config().fromTableName(),
                rowCount,
                chunk.startRowId(),
                chunk.endRowId(),
                chunk.chunkId());
*/
    }

    public LogMessage saveToLogger(Chunk chunk,
                                   int recordCount,
                                   long start,
                                   String operation) {
        LogMessage logMessage = new LogMessage(
                chunk.config().fromTaskName(),
                chunk.config().fromTableName(),
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

/*
    private Boolean hasLOB(ResultSet fetchResultSet) throws SQLException {
        for (int i = 1; i <= fetchResultSet.getMetaData().getColumnCount(); i++) {
            int columnType = fetchResultSet.getMetaData().getColumnType(i);
            if (columnType == 2004 || columnType == 2005) {
                return true;
            }
        }
        return false;
    }
*/
}
