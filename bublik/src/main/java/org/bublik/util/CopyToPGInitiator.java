package org.bublik.util;

import de.bytefish.pgbulkinsert.row.SimpleRow;
import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.bublik.Bublik;
import org.bublik.exception.TableNotExistsException;
import org.bublik.model.*;
import org.bublik.service.ChunkService;
import org.bublik.service.TableService;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.*;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Consumer;

import static org.bublik.util.ColumnUtil.*;

public class CopyToPGInitiator {
    private static final Logger LOGGER = LoggerFactory.getLogger(Bublik.class);

    public LogMessage start(ResultSet fetchResultSet) throws SQLException {
        Chunk<?> chunk = ChunkService.get();
        LogMessage logMessage;
        if (fetchResultSet.next()) {
            Connection connection = DatabaseUtil.getPoolConnectionDbTo();
            Table table = TableService.getTable(connection, chunk.getConfig().toSchemaName(), chunk.getConfig().toTableName());
            if (table.exists(connection)) {
                Chunk<?> ch = chunk.buildChunkWithTargetTable(chunk, table);
                try {
                    logMessage = fetchAndCopy(connection, fetchResultSet, ch);
                } catch (SQLException | RuntimeException e) {
                    connection.close();
                    throw e;
                }
            } else {
                LOGGER.error("\u001B[31mThe Target Table: {}.{} does not exist.\u001B[0m", chunk.getConfig().toSchemaName(),
                        chunk.getConfig().toTableName());
                throw new TableNotExistsException("The Target Table "
                        + chunk.getConfig().toSchemaName() + "."
                        + chunk.getConfig().toTableName() + " does not exist.");
            }
            connection.close();
        } else {
             logMessage = new LogMessage(
                    0,
                    System.currentTimeMillis(),
                    "NO ROWS FETCH",
                    chunk);
        }
        return logMessage;
    }

    private LogMessage fetchAndCopy(Connection connection,
                                    ResultSet fetchResultSet,
                                    Chunk<?> chunk) throws SQLException {
        int recordCount = 0;
        Map<String, PGColumn> neededColumnsToDB = readTargetColumnsAndTypes(connection, chunk);
        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connection);
        String[] columnNames = neededColumnsToDB
                .values()
                .stream()
                .map(PGColumn::getColumnName)
                .toList()
                .toArray(String[]::new);
        SimpleRowWriter.Table table =
                new SimpleRowWriter.Table(chunk.getTargetTable().getSchemaName(),
                        chunk.getTargetTable().getFinalTableName(true), columnNames);
        long start = System.currentTimeMillis();
//        try (SimpleRowWriter writer = new SimpleRowWriter(table, pgConnection)) {
        SimpleRowWriter writer = new SimpleRowWriter(table, pgConnection);
        Consumer<SimpleRow> simpleRowConsumer =
                s -> {
                    try {
                        simpleRowConsume(s, neededColumnsToDB, fetchResultSet, chunk, connection, writer);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                };
        do {
            writer.startRow(simpleRowConsumer);
            recordCount++;
        } while (fetchResultSet.next());
        writer.close();
        connection.commit();
        return new LogMessage(
                recordCount,
                start,
                "COPY",
                chunk);
    }

    private void simpleRowConsume(SimpleRow row,
                                  Map<String, PGColumn> neededColumnsToDB,
                                  ResultSet fetchResultSet,
                                  Chunk<?> chunk,
                                  Connection connection,
                                  SimpleRowWriter writer) throws SQLException {
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
                case "varchar": {
                    String s = fetchResultSet.getString(sourceColumn);
                    if (s == null) {
                        row.setText(targetColumn, null);
                        break;
                    }
                    row.setText(targetColumn, s.replaceAll("\u0000", ""));
                    break;
                }
                case "bpchar":
                    String string = fetchResultSet.getString(sourceColumn);
                    row.setText(targetColumn, string);
                    break;
                case "text": {
                    Object o = fetchResultSet.getObject(sourceColumn);
                    if (o == null) {
                        row.setText(targetColumn, null);
                        break;
                    }
                    String text;
                    int cIndex = getColumnIndexByColumnName(fetchResultSet, sourceColumn);
                    if (cIndex != 0 && fetchResultSet.getMetaData().getColumnType(cIndex) == 2005) {
                        text = convertClobToString(fetchResultSet, sourceColumn);
                    } else {
                        text = fetchResultSet.getString(sourceColumn);
                    }
                    row.setText(targetColumn, text);
                    break;
                }
                case "jsonb": {
                    Object o = fetchResultSet.getObject(sourceColumn);
                    if (o == null) {
                        row.setJsonb(targetColumn, null);
                        break;
                    }
                    String s = null;
                    if (chunk instanceof OraChunk<?>) {
                        int columnIndex = getColumnIndexByColumnName(fetchResultSet, sourceColumn.toUpperCase());
                        int columnType = fetchResultSet.getMetaData().getColumnType(columnIndex);
                        switch (columnType) {
                            // CLOB
                            case 2005:
                                s = convertClobToString(fetchResultSet, sourceColumn);
                                break;
                            default:
                                s = fetchResultSet.getString(sourceColumn);
                                break;
                        }
                    } else if (chunk instanceof PGChunk<?>) {
                        s = fetchResultSet.getString(sourceColumn);
                    }
                    row.setJsonb(targetColumn, s);
                    break;
                }
                case "smallserial", "int2": {
                    Object o = fetchResultSet.getObject(sourceColumn);
                    if (o == null) {
                        row.setShort(targetColumn, null);
                        break;
                    }
                    Short aShort = fetchResultSet.getShort(sourceColumn);
                    row.setShort(targetColumn, aShort);
                    break;
                }
                case "serial", "int4": {
                    Object o = fetchResultSet.getObject(sourceColumn);
                    if (o == null) {
                        row.setInteger(targetColumn, null);
                        break;
                    }
                    int i = fetchResultSet.getInt(sourceColumn);
                    row.setInteger(targetColumn, i);
                    break;
                }
                case "bigint", "int8": {
                    Object o = fetchResultSet.getObject(sourceColumn);
                    if (o == null) {
                        row.setLong(targetColumn, null);
                        break;
                    }
                    long l = fetchResultSet.getLong(sourceColumn);
                    row.setLong(targetColumn, l);
                    break;
                }
                case "numeric": {
                    Object o = fetchResultSet.getObject(sourceColumn);
                    if (o == null) {
                        row.setNumeric(targetColumn, null);
                        break;
                    }
                    row.setNumeric(targetColumn, (Number) o);
                    break;
                }
                case "float4": {
                    Object o = fetchResultSet.getObject(sourceColumn);
                    if (o == null) {
                        row.setDouble(targetColumn, null);
                        break;
                    }
                    Float aFloat = fetchResultSet.getFloat(sourceColumn);
                    row.setFloat(targetColumn, aFloat);
                    break;
                }
                case "float8": {
                    Object o = fetchResultSet.getObject(sourceColumn);
                    if (o == null) {
                        row.setDouble(targetColumn, null);
                        break;
                    }
                    Double aDouble = fetchResultSet.getDouble(sourceColumn);
                    row.setDouble(targetColumn, aDouble);
                    break;
                }
                case "time", "timestamp": {
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
                }
                case "timestamptz": {
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
                }
                case "date":
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
                case "bytea": {
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
                }
                case "bool": {
                    Object o = fetchResultSet.getObject(sourceColumn);
                    if (o == null) {
                        row.setBoolean(targetColumn, null);
                        break;
                    }
                    boolean b = fetchResultSet.getBoolean(sourceColumn);
                    row.setBoolean(targetColumn, b);
                    break;
                }
                case "uuid":
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
                default:
                    try {
                        String s = fetchResultSet.getString(sourceColumn);
                        if (s == null) {
                            row.setText(targetColumn, null);
                            break;
                        }
                        row.setText(targetColumn, s.replaceAll("\u0000", ""));
                        break;
                    } catch (Exception e) {
                        LOGGER.error("\u001B[31mThere is no handler for type : {}\u001B[0m", targetType);
                        writer.close();
                        connection.close();
                    }
            }
        }
    }
}
