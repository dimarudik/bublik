package org.example.util;

import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import lombok.extern.slf4j.Slf4j;
import org.example.model.*;
import org.example.service.ChunkService;
import org.example.service.TableService;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.*;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import static org.example.util.ColumnUtil.*;

@Slf4j
public class CopyToPGInitiator {
    private StringBuffer tmpString = new StringBuffer();

    public RunnerResult initiateProcessToDatabase(ResultSet fetchResultSet) {
        LogMessage logMessage = null;
        Chunk<?> chunk = ChunkService.get();
        try (Connection connection = DatabaseUtil.getConnectionDbTo()) {
            if (fetchResultSet.next()) {
                Table table = TableService.getTable(connection, chunk.getConfig().toSchemaName(), chunk.getConfig().toTableName());
                if (table.exists(connection)) {
                    Chunk<?> ch = chunk.buildChunkWithTargetTable(chunk, table);
                    logMessage = fetchAndCopy(connection, fetchResultSet, ch);
                }
            } else {
                logMessage = saveToLogger(
                        chunk,
                        0,
                        System.currentTimeMillis(),
                        "NO ROWS FETCH");
            }
        } catch (SQLException e) {
            log.error("{} \t {}", chunk.getConfig().fromTableName(), e);
            return new RunnerResult(logMessage, e);
        }
        return new RunnerResult(logMessage, null);
    }

    private LogMessage fetchAndCopy(Connection connection,
                                    ResultSet fetchResultSet,
                                    Chunk<?> chunk) {
        int rowCount = 0;
        Map<String, PGColumn> neededColumnsToDB = readTargetColumnsAndTypes(connection, chunk);
        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connection);
        String[] columnNames = neededColumnsToDB
                .values()
                .stream()
                .map(PGColumn::getColumnName)
                .toList()
                .toArray(new String[0]);
        SimpleRowWriter.Table table =
                new SimpleRowWriter.Table(chunk.getTargetTable().getSchemaName(),
                        chunk.getTargetTable().getFinalTableName(true), columnNames);
        long start = System.currentTimeMillis();
        try (SimpleRowWriter writer = new SimpleRowWriter(table, pgConnection)) {
            do {
                writer.startRow((row) -> {
                    tmpString.setLength(0);
                    for (Map.Entry<String, PGColumn> entry : neededColumnsToDB.entrySet()) {
                        String sourceColumn = entry.getKey().replaceAll("\"", "");
                        String targetColumn = entry.getValue().getColumnName();
                        String targetType = entry.getValue().getColumnType();

/*
                        try {
                            tmpString
                                    .append(targetType)
                                    .append(" : ")
                                    .append(targetColumn)
                                    .append(" : ")
                                    .append(fetchResultSet.getString(sourceColumn)).append("\n");
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
*/

                        switch (targetType) {
                            case "varchar":
                                try {
                                    String s = fetchResultSet.getString(sourceColumn);
                                    if (s == null) {
                                        row.setText(targetColumn, null);
                                        break;
                                    }
                                    row.setText(targetColumn, s.replaceAll("\u0000", ""));
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "bpchar":
                                try {
                                    String s = fetchResultSet.getString(sourceColumn);
                                    row.setText(targetColumn, s);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "text":
                                try {
                                    Object o = fetchResultSet.getObject(sourceColumn);
                                    if (o == null) {
                                        row.setText(targetColumn, null);
                                        break;
                                    }
                                    String s;
                                    int cIndex = getColumnIndexByColumnName(fetchResultSet, sourceColumn);
                                    if (cIndex != 0 && fetchResultSet.getMetaData().getColumnType(cIndex) == 2005) {
                                        s = convertClobToString(fetchResultSet, sourceColumn);
                                    } else {
                                        s = fetchResultSet.getString(sourceColumn);
                                    }
                                    row.setText(targetColumn, s);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "jsonb":
                                try {
                                    String s = fetchResultSet.getString(sourceColumn);
                                    if (s == null) {
                                        row.setText(targetColumn, null);
                                        break;
                                    }
                                    row.setJsonb(targetColumn, s);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "smallserial":
                                try {
                                    Object o = fetchResultSet.getObject(sourceColumn);
                                    if (o == null) {
                                        row.setShort(targetColumn, null);
                                        break;
                                    }
                                    Short aShort = fetchResultSet.getShort(sourceColumn);
                                    row.setShort(targetColumn, aShort);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "serial":
                                try {
                                    Object o = fetchResultSet.getObject(sourceColumn);
                                    if (o == null) {
                                        row.setInteger(targetColumn, null);
                                        break;
                                    }
                                    int i = fetchResultSet.getInt(sourceColumn);
                                    row.setInteger(targetColumn, i);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "bigint":
                                try {
                                    Object o = fetchResultSet.getObject(sourceColumn);
                                    if (o == null) {
                                        row.setLong(targetColumn, null);
                                        break;
                                    }
                                    long l = fetchResultSet.getLong(sourceColumn);
                                    row.setLong(targetColumn, l);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "numeric":
                                try {
                                    Object o = fetchResultSet.getObject(sourceColumn);
                                    if (o == null) {
                                        row.setNumeric(targetColumn, null);
                                        break;
                                    }
                                    row.setNumeric(targetColumn, (Number) o);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "int2":
                                try {
                                    Object o = fetchResultSet.getObject(sourceColumn);
                                    if (o == null) {
                                        row.setShort(targetColumn, null);
                                        break;
                                    }
                                    Short aShort = fetchResultSet.getShort(sourceColumn);
                                    row.setShort(targetColumn, aShort);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "int4":
                                try {
                                    Object o = fetchResultSet.getObject(sourceColumn);
                                    if (o == null) {
                                        row.setInteger(targetColumn, null);
                                        break;
                                    }
                                    int i = fetchResultSet.getInt(sourceColumn);
                                    row.setInteger(targetColumn, i);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "int8":
                                try {
                                    Object o = fetchResultSet.getObject(sourceColumn);
                                    if (o == null) {
                                        row.setLong(targetColumn, null);
                                        break;
                                    }
                                    long l = fetchResultSet.getLong(sourceColumn);
                                    row.setLong(targetColumn, l);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "float4":
                                try {
                                    Object o = fetchResultSet.getObject(sourceColumn);
                                    if (o == null) {
                                        row.setDouble(targetColumn, null);
                                        break;
                                    }
                                    Float aFloat = fetchResultSet.getFloat(sourceColumn);
                                    row.setFloat(targetColumn, aFloat);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "float8":
                                try {
                                    Object o = fetchResultSet.getObject(sourceColumn);
                                    if (o == null) {
                                        row.setDouble(targetColumn, null);
                                        break;
                                    }
                                    Double aDouble = fetchResultSet.getDouble(sourceColumn);
                                    row.setDouble(targetColumn, aDouble);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "time":
                                try {
                                    Timestamp timestamp = fetchResultSet.getTimestamp(sourceColumn);
                                    if (timestamp == null) {
                                        row.setTimeStamp(targetColumn, null);
                                        break;
                                    }
                                    long l = timestamp.getTime();
                                    LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(l),
                                            TimeZone.getDefault().toZoneId());
                                    row.setTimeStamp(targetColumn, localDateTime);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "timestamp":
                                try {
                                    Timestamp timestamp = fetchResultSet.getTimestamp(sourceColumn);
                                    if (timestamp == null) {
                                        row.setTimeStamp(targetColumn, null);
                                        break;
                                    }
                                    long l = timestamp.getTime();
                                    LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(l),
                                            TimeZone.getDefault().toZoneId());
                                    row.setTimeStamp(targetColumn, localDateTime);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "timestamptz":
                                try {
                                    Timestamp timestamp = fetchResultSet.getTimestamp(sourceColumn);
                                    if (timestamp == null) {
                                        row.setTimeStamp(targetColumn, null);
                                        break;
                                    }
                                    ZonedDateTime zonedDateTime =
                                            ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp.getTime()),
                                                    ZoneOffset.UTC);
                                    row.setTimeStampTz(targetColumn, zonedDateTime);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "date":
                                try {
                                    Timestamp timestamp = fetchResultSet.getTimestamp(sourceColumn);
                                    if (timestamp == null) {
                                        row.setDate(targetColumn, null);
                                        break;
                                    }
                                    long l = timestamp.getTime();
                                    LocalDate localDate = Instant.ofEpochMilli(l)
                                            .atZone(ZoneId.systemDefault()).toLocalDate();
                                    row.setDate(targetColumn, localDate);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "bytea":
                                try {
                                    Object o = fetchResultSet.getObject(sourceColumn);
                                    if (o == null) {
                                        row.setByteArray(targetColumn, null);
                                        break;
                                    }
                                    byte[] bytes = new byte[0];
                                    if (chunk instanceof OraChunk<?>) {
                                        int columnIndex = getColumnIndexByColumnName(fetchResultSet, sourceColumn.toUpperCase());
                                        int columnType = fetchResultSet.getMetaData().getColumnType(columnIndex);
                                        switch (columnType) {
                                            // RAW
                                            case -3:
                                                bytes = fetchResultSet.getBytes(sourceColumn);
                                                break;
                                            // LONG RAW
                                            case -4:
                                                bytes = fetchResultSet.getBytes(sourceColumn);
                                                break;
                                            // BLOB
                                            case 2004:
                                                bytes = convertBlobToBytes(fetchResultSet, sourceColumn);
                                                break;
                                            default:
                                                break;
                                        }
                                    } else if (chunk instanceof PGChunk<?>) {
                                        bytes = fetchResultSet.getBytes(sourceColumn);
                                    }
                                    row.setByteArray(targetColumn, bytes);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "bool":
                                try {
                                    Object o = fetchResultSet.getObject(sourceColumn);
                                    if (o == null) {
                                        row.setBoolean(targetColumn, null);
                                        break;
                                    }
                                    boolean b = fetchResultSet.getBoolean(sourceColumn);
                                    row.setBoolean(targetColumn, b);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            case "uuid":
                                try {
                                    Object o = fetchResultSet.getObject(sourceColumn);
                                    if (o == null) {
                                        row.setUUID(targetColumn, null);
                                        break;
                                    }
                                    UUID uuid = null;
                                    try {
                                        uuid = (UUID) o;
                                    } catch (ClassCastException e) {
                                        uuid = UUID.fromString((String) o);
                                    }
                                    row.setUUID(targetColumn, uuid);
                                    break;
                                } catch (SQLException e) {
                                    log.error("{} {}", entry.getKey(), e);
                                }
                            default:
                                throw new RuntimeException("There is no handler for type : " + entry.getValue());
                        }
                    }
                });
                rowCount++;
            } while (fetchResultSet.next());
        } catch (SQLException | RuntimeException e) {
            System.out.println(tmpString);
            e.printStackTrace();
            log.error(e.getMessage(), e);
            return null;
        }
        return saveToLogger(chunk, rowCount, start, "COPY");
    }

    public LogMessage saveToLogger(Chunk<?> chunk,
                                   int recordCount,
                                   long start,
                                   String operation) {
        LogMessage logMessage = new LogMessage(
                chunk.getConfig().fromTaskName(),
                chunk.getConfig().fromTableName(),
                recordCount,
                chunk.getStart().toString(),
                chunk.getEnd().toString(),
                chunk.getId());
        log.info("\t{} {}\t {} sec",
                operation,
                logMessage,
                Math.round((float) (System.currentTimeMillis() - start) / 10) / 100.0);
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
