package org.bublik.postgres.storage;

import de.bytefish.pgbulkinsert.exceptions.BinaryWriteFailedException;
import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.pgsql.model.interval.Interval;
import de.bytefish.pgbulkinsert.pgsql.model.range.Range;
import de.bytefish.pgbulkinsert.row.SimpleRow;
import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.constants.PGKeywords;
import org.bublik.core.exception.SourceSQLException;
import org.bublik.core.exception.TableNotExistsException;
import org.bublik.core.exception.TargetSQLException;
import org.bublik.core.model.*;
import org.bublik.core.storage.JDBCStorage;
import org.bublik.core.storage.Storage;
import org.bublik.core.storage.StorageClass;
import org.bublik.postgres.model.PGChunk;
import org.bublik.postgres.model.PGTable;
import org.bublik.postgres.util.ColumnUtil;
import org.postgresql.PGConnection;
import org.postgresql.core.Encoding;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.util.HStoreConverter;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.util.*;
import java.util.function.Consumer;

import static org.bublik.core.constants.CLassConstants.ORACLE_STORAGE_CLASS_NAME;
import static org.bublik.core.util.ColumnUtil.*;
import static org.bublik.core.util.Utils.getStackTrace;
import static org.bublik.postgres.constants.SQLConstants.*;
import static org.bublik.postgres.util.ColumnUtil.*;

public class JDBCPostgreSQLStorage<K extends Integer, T extends Long, S extends Connection, R extends ResultSet> extends JDBCStorage<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(JDBCPostgreSQLStorage.class);

    public JDBCPostgreSQLStorage(StorageClass storageClass, ConnectionProperty connectionProperty) throws SQLException {
        super(storageClass, connectionProperty);
    }

    @Override
    public List<Chunk<K, T, S, R>> getChunkList(List<Config> configs, String chunkTableName, Storage<K, T, S, R> targetStorage) throws SQLException {
        List<Chunk<K, T, S, R>> chunks = new ArrayList<>();
        Connection connection = getConnection();
        String sql = buildStartEndOfChunk(configs, chunkTableName);
        log.debug("SQL to fetch metadata of chunks: \n{}", sql);
        Map<String, Table> tableMap = new HashMap<>();
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet rs = statement.executeQuery();
        if (rs.isBeforeFirst()) {
            while (rs.next()) {
                Config config = findByTaskName(configs, rs.getString("task_name"));
                Table sourceTable = getTables()
                        .entrySet()
                        .stream()
                        .filter(s -> s.getKey().getSchemaName().equalsIgnoreCase(config.fromSchemaName())
                                && s.getKey().getTableName().equalsIgnoreCase(config.fromTableName()))
                        .findFirst()
                        .orElseThrow(() -> new TableNotExistsException("Table " +
                                config.fromSchemaName() + "." +
                                config.fromTableName() + " not exists in cache"))
                        .getKey();
                String status = rs.getString("status");
                Chunk <K, T, S, R> chunk = new PGChunk<>(
                        (K)(Integer)rs.getInt("chunk_id"),
                        (T)(Long)rs.getLong("start_page"),
                        (T)(Long)rs.getLong("end_page"),
                        config,
                        sourceTable,
                        ChunkStatus.valueOf(status),
                        null,
                        this,
                        targetStorage);
                chunks.add(chunk);
                String query;
                if (config.columnToColumn() == null && config.expressionToColumn() == null) {
                    query = buildFetchStatement(config, chunk);
                } else {
                    query = buildFetchStatement(config);
                }
                tableMap.put(query, sourceTable);
            }
        }
        tableMap.keySet().forEach(s -> log.info("{}", s));
        rs.close();
        statement.close();
        return chunks;
    }

    @Override
    public String buildStartEndOfChunk(List<Config> configs, String chunkTableName) {
        List<String> taskNames = new ArrayList<>();
        configs.forEach(sqlStatement -> taskNames.add(sqlStatement.fromTaskName()));
        return "select row_number() over (order by chunk_id) as rownum, chunk_id, uuid, start_page, end_page, task_name, status from " +
                chunkTableName + " where task_name in ('" +
                String.join("', '", taskNames) + "') " +
                // тут надо разбираться при запуске из нескольких подов
                "and status in ('ASSIGNED', 'UNASSIGNED', 'PROCESSED_WITH_ERROR') "
                + " limit 1000 "
                ;
    }

    @Override
    public LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException, BinaryWriteFailedException,
            SourceSQLException, TargetSQLException {
        ResultSet fetchResultSet = (ResultSet) chunk.getResultSet();
        Connection connectionFrom = (Connection) chunk.getSourceSession();
        if (fetchResultSet.next()) {
            Connection connectionTo = (Connection) chunk.getTargetSession();
            Table table = configToTable(chunk.getConfig().toSchemaName(), chunk.getConfig().toTableName());
            chunk.setTargetTable(table);
            try {
                LogMessage logMessage = fetchAndCopy(fetchResultSet, chunk, tableName);
                connectionTo.close();
                return logMessage;
            } catch (SQLException e) {
                connectionTo.rollback();
                connectionTo.close();
                throw e;
            } catch (SourceSQLException s) {
                connectionFrom.close();
                log.error("{}", getStackTrace(s));
                throw s;
            } catch (BinaryWriteFailedException b) {
                if (b.getCause() instanceof PSQLException && b.getCause().getCause() == null) {
                    connectionTo.close();
                }
                throw b;
            } finally {
                ;
            }
        } else {
            return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), "NO ROWS FETCH");
        }
    }

    private LogMessage fetchAndCopy(ResultSet fetchResultSet,
                                    Chunk<?, ?, ?, ?> chunk,
                                    String tableName) throws SQLException, BinaryWriteFailedException, SourceSQLException{
        int recordCount = 0;
        Connection connectionTo = (Connection) chunk.getTargetSession();
//        Connection connectionTo = chunk.getTargetConnection();

        try {
            insertProcessedChunkInfo(connectionTo, (int) chunk.getId(), recordCount, chunk.getConfig().fromTaskName(), tableName);
            connectionTo.rollback();
        } catch (PSQLException p) {
            log.error("{}", getStackTrace(p));
            connectionTo.rollback();
            return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), "The chunk has already been copied");
        }

        Map<String, Column> columnToColumnMap = readTargetColumnsAndTypes(connectionTo, chunk);
        Map<List<String>, Column> neededColumnsFromMany = readTargetColumnsAndTypesFromMany(connectionTo, chunk);

        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connectionTo);

        String[] columnNames = columnToColumnMap
                .values()
                .stream()
                .map(Column::columnName)
                .toList()
                .toArray(String[]::new);
        String[] cNames = Arrays.copyOf(columnNames, columnNames.length);
        SimpleRowWriter.Table table =
                new SimpleRowWriter.Table(chunk.getTargetTable().getSchemaName(),
                        chunk.getTargetTable().getFinalTableName(true), cNames);

        SimpleRowWriter writer = new SimpleRowWriter(table, pgConnection);
        Consumer<SimpleRow> simpleRowConsumer =
            s -> {
                try {
                    simpleRowConsume(s, columnToColumnMap, neededColumnsFromMany,
                            fetchResultSet, chunk, connectionTo, writer);
                } catch (BinaryWriteFailedException | SQLException e) {
                    log.error("{}.{} {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                }
            };

        do {
            try {
                writer.startRow(simpleRowConsumer);
            } catch (BinaryWriteFailedException e) {
//                LOGGER.error("BinaryWriteFailedException caught by writer.startRow() {}", getStackTrace(e));
                throw e;
            }
            recordCount++;
        } while (hasNext(fetchResultSet));

        try {
            writer.close();
        } catch (BinaryWriteFailedException b) {
            throw b;
        }

        chunk.setRows(recordCount);
        insertProcessedChunkInfo(connectionTo, (int) chunk.getId(), recordCount, chunk.getConfig().fromTaskName(), tableName);
        connectionTo.commit();

        return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), "PostgreSQL COPY");
    }

    private boolean hasNext(ResultSet resultSet) throws SourceSQLException {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            throw new SourceSQLException(getStackTrace(e));
        }
    }

    @Override
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?, ?, ?> chunk) {
        Map<String, Column> columnMap = new HashMap<>();
        try {
            ResultSet resultSet = connectionTo.getMetaData().getColumns(
                    null,
                    chunk.getTargetTable().getSchemaName().toLowerCase(),
                    chunk.getTargetTable().getFinalTableName(false),
                    null);
            Map<String, String> columnToColumnMap = chunk.getConfig().columnToColumn();
            Map<String, String> expressionToColumnMap = chunk.getConfig().expressionToColumn();
            Map<String, List<String>> columnFromManyMap = chunk.getConfig().columnFromMany();

            while (resultSet.next()) {
                String columnName = resultSet.getString(4);
                Integer dataType = resultSet.getInt(5);
                String columnType = resultSet.getString(6);
                Integer columnPosition = resultSet.getInt(17);

                if (columnToColumnMap != null) {
                    columnToColumnMap
                            .entrySet()
                            .stream()
                            .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(columnName))
                            .forEach(i -> columnMap.put(i.getKey(),
                                    new Column(
                                            columnPosition,
                                            i.getValue(),
                                            columnType.equals("bigserial") ? "bigint" : columnType,
                                            dataType, null, null, null, null, 0, null, 0, null)));
                } else if (expressionToColumnMap == null) {
                    Table<?> sourceTable = chunk.getSourceTable();
                    sourceTable.getColumns().forEach(column -> columnMap.put(column.columnName(), column));
                }

                if (expressionToColumnMap != null) {
                    expressionToColumnMap
                            .entrySet()
                            .stream()
                            .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(columnName))
                            .forEach(i -> columnMap.put(columnName,
                                    new Column(
                                            columnPosition,
                                            i.getValue(),
                                            columnType.equals("bigserial") ? "bigint" : columnType,
                                            dataType, null, null, null, null, 0 , null, 0, null)));
                }

                if (columnFromManyMap != null) {
                    columnFromManyMap
                            .entrySet()
                            .stream()
                            .filter(s -> s.getKey().replaceAll("\"", "").equalsIgnoreCase(columnName))
                            .forEach(i -> columnMap.put(i.getKey(),
                                    new Column(
                                            columnPosition,
                                            i.getKey(),
                                            columnType.equals("bigserial") ? "bigint" : columnType,
                                            dataType, null, null, null, null, 0 , null, 0, null)));
                }
            }
            resultSet.close();
        } catch (SQLException e) {
            log.error("{}", e.getMessage());
        }
        return columnMap;
    }

    protected Map<List<String>, Column> readTargetColumnsAndTypesFromMany(Connection connectionTo, Chunk<?, ?, ?, ?> chunk) {
        Map<List<String>, Column> columnMap = new HashMap<>();
        try {
            ResultSet resultSet = connectionTo.getMetaData().getColumns(
                    null,
                    chunk.getTargetTable().getSchemaName().toLowerCase(),
                    chunk.getTargetTable().getFinalTableName(false),
                    null);
            Map<String, List<String>> columnFromManyMap = chunk.getConfig().columnFromMany();

            while (resultSet.next()) {
                String columnName = resultSet.getString(4);
                Integer dataType = resultSet.getInt(5);
                String columnType = resultSet.getString(6);
                Integer columnPosition = resultSet.getInt(17);

                if (columnFromManyMap != null) {
                    columnFromManyMap
                            .entrySet()
                            .stream()
                            .filter(s -> s.getKey().replaceAll("\"", "").equalsIgnoreCase(columnName))
                            .forEach(i -> columnMap.put(i.getValue(),
                                    new Column(
                                            columnPosition,
                                            i.getKey(),
                                            columnType.equals("bigserial") ? "bigint" : columnType,
                                            dataType, null, null, null, null, 0 , null, 0, null)));
                }
            }
            resultSet.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
        return columnMap;
    }

    private void simpleRowConsume(SimpleRow row,
                                  Map<String, Column> neededColumnsToDB,
                                  Map<List<String>, Column> neededColumnsFromMany,
                                  ResultSet fetchResultSet,
                                  Chunk<?, ?, ?, ?> chunk,
                                  Connection connectionTo,
                                  SimpleRowWriter writer) throws SQLException, BinaryWriteFailedException {
        for (Map.Entry<String, Column> entry : neededColumnsToDB.entrySet()) {
            String sourceColumn = entry.getKey().replaceAll("\"", "");
            String targetColumn = entry.getValue().columnName();
            String targetType = entry.getValue().columnType();

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
                case "hstore": {
                    try {
                        String s = fetchResultSet.getString(sourceColumn);
                        if (s == null) {
                            row.setHstore(targetColumn, null);
                            break;
                        }
                        Map<String, String> hstoreMap = parseHstoreString(s);
                        row.setHstore(targetColumn, hstoreMap);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "json", "varchar": {
                    try {
                        String s = fetchResultSet.getString(sourceColumn);
                        if (s == null) {
                            row.setVarChar(targetColumn, null);
                            break;
                        }
                        row.setVarChar(targetColumn, s.replaceAll("\u0000", ""));
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "_varchar": {
                    try {
                        Object s = fetchResultSet.getObject(sourceColumn);
                        if (s == null) {
                            row.setVarCharArray(targetColumn, new ArrayList<>());
                            break;
                        }
                        List<String> arr = List.of(((String[]) fetchResultSet.getArray(sourceColumn).getArray()));
                        row.setVarCharArray(targetColumn, arr);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "_text": {
                    try {
                        Object s = fetchResultSet.getObject(sourceColumn);
                        if (s == null) {
                            row.setTextArray(targetColumn, null);
                            break;
                        }
                        List<String> arr = List.of(((String[]) fetchResultSet.getArray(sourceColumn).getArray()));
                        row.setTextArray(targetColumn, arr);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "bpchar":
                    try {
                        String string = fetchResultSet.getString(sourceColumn);
                        row.setText(targetColumn, string);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                case "text": {
                    try {
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
                        row.setText(targetColumn, text.replaceAll("\u0000", ""));
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "jsonb": {
                    try {
                    Object o = fetchResultSet.getObject(sourceColumn);
                    if (o == null) {
                        row.setJsonb(targetColumn, null);
                        break;
                    }
                    String s = null;
                    if (chunk.getSourceStorage().getClass().getName().equals(ORACLE_STORAGE_CLASS_NAME)) {
                            int columnIndex = getColumnIndexByColumnName(fetchResultSet, sourceColumn.toUpperCase());
                            int columnType = fetchResultSet.getMetaData().getColumnType(columnIndex);
                            switch (columnType) {
                                // CLOB
                                case 2005:
                                    s = convertClobToString(fetchResultSet, sourceColumn).replaceAll("\u0000", "");
                                    break;
                                // NCLOB
                                case 2011:
                                    s = convertClobToString(fetchResultSet, sourceColumn).replaceAll("\u0000", "");
                                    break;
                                default:
                                    s = fetchResultSet.getString(sourceColumn).replaceAll("\u0000", "");
                                    break;
                            }
                        } else if (chunk instanceof PGChunk<?, ?, ?, ?>) {
                            s = fetchResultSet.getString(sourceColumn);
                        }
                        row.setJsonb(targetColumn, s);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "smallserial", "int2": {
                    try {
                        Object o = fetchResultSet.getObject(sourceColumn);
                        if (o == null) {
                            row.setShort(targetColumn, null);
                            break;
                        }
                        Short aShort = fetchResultSet.getShort(sourceColumn);
                        row.setShort(targetColumn, aShort);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "serial", "int4": {
                    try {
                        Object o = fetchResultSet.getObject(sourceColumn);
                        if (o == null) {
                            row.setInteger(targetColumn, null);
                            break;
                        }
                        int i = fetchResultSet.getInt(sourceColumn);
                        row.setInteger(targetColumn, i);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "bigint", "int8": {
                    try {
                        Object o = fetchResultSet.getObject(sourceColumn);
                        if (o == null) {
                            row.setLong(targetColumn, null);
                            break;
                        }
                        long l = fetchResultSet.getLong(sourceColumn);
                        row.setLong(targetColumn, l);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} {} -> {}: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
                        throw e;
                    }
                }
                case "numeric": {
                    try {
                        Object o = fetchResultSet.getObject(sourceColumn);
                        if (o == null) {
                            row.setNumeric(targetColumn, null);
                            break;
                        }
                        row.setNumeric(targetColumn, (Number) o);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} {} -> {}: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
                        throw e;
                    }
                }
                case "float4": {
                    try {
                        Object o = fetchResultSet.getObject(sourceColumn);
                        if (o == null) {
                            row.setDouble(targetColumn, null);
                            break;
                        }
                        Float aFloat = fetchResultSet.getFloat(sourceColumn);
                        row.setFloat(targetColumn, aFloat);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} {} -> {}: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
                        throw e;
                    }
                }
                case "float8": {
                    try {
                        Object o = fetchResultSet.getObject(sourceColumn);
                        if (o == null) {
                            row.setDouble(targetColumn, null);
                            break;
                        }
                        Double aDouble = fetchResultSet.getDouble(sourceColumn);
                        row.setDouble(targetColumn, aDouble);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} {} -> {}: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
                        throw e;
                    }
                }
                case "time": {
                    try {
                        Time time = fetchResultSet.getTime(sourceColumn);
                        if (time == null) {
                            row.setTimeStamp(targetColumn, null);
                            break;
                        }
                        long l = time.getTime();
                        LocalTime localTime = LocalTime.ofInstant(Instant.ofEpochMilli(l),
                                TimeZone.getDefault().toZoneId());
                        row.setValue(targetColumn, DataType.Time, localTime);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "timestamp": {
                    try {
                        Timestamp timestamp = fetchResultSet.getTimestamp(sourceColumn);
                        if (timestamp == null) {
                            row.setTimeStamp(targetColumn, null);
                            break;
                        }
                        LocalDateTime localDateTime = timestamp.toLocalDateTime();
                        row.setTimeStamp(targetColumn, localDateTime);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "timestamptz": {
                    try {
                        Timestamp timestamp = fetchResultSet.getTimestamp(sourceColumn);
                        if (timestamp == null) {
                            row.setTimeStamp(targetColumn, null);
                            break;
                        }
/*
                        ZonedDateTime zonedDateTime =
                                ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp.getTime()),
                                        ZoneOffset.UTC);
*/
                        ZonedDateTime zonedDateTime =
                                ZonedDateTime.ofInstant(timestamp.toInstant(), ZoneId.of("UTC"));
                        row.setTimeStampTz(targetColumn, zonedDateTime);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "date":
                    try {
                        Date date = fetchResultSet.getDate(sourceColumn);
                        if (date == null) {
                            row.setDate(targetColumn, null);
                            break;
                        }
                        row.setDate(targetColumn, date.toLocalDate());
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                case "tstzrange":
                    try {
                        List<String> sourceColumns = neededColumnsFromMany
                                .entrySet()
                                .stream()
                                .filter(i -> i.getValue().columnName().equals(targetColumn))
                                .map(Map.Entry::getKey)
                                .toList().getLast();
                        Timestamp start = fetchResultSet.getTimestamp(sourceColumns.getFirst());
                        Timestamp end = fetchResultSet.getTimestamp(sourceColumns.getLast());
                        ZonedDateTime lowerBound = null;
                        if (start != null) {
                             lowerBound = ZonedDateTime.ofInstant(start.toInstant(), ZoneId.of("UTC"));
                        }
                        ZonedDateTime upperBound = null;
                        if (end != null) {
                            upperBound = ZonedDateTime.ofInstant(end.toInstant(), ZoneId.of("UTC"));
                        }
                        Range<ZonedDateTime> localDateTimeRange = new Range<>(
                                lowerBound,
                                true,
                                lowerBound == null,
                                upperBound,
                                true,
                                upperBound == null);
                        row.setTsTzRange(targetColumn, localDateTimeRange);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("tstzrange : {}.{} - {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                case "interval":
                    try {
                        Object o = fetchResultSet.getObject(sourceColumn);
                        if (o == null) {
                            row.setDouble(targetColumn, null);
                            break;
                        }
                        Interval interval = null;
                        if (chunk.getSourceStorage().getClass().getName().equals(ORACLE_STORAGE_CLASS_NAME)) {
                            int columnIndex = getColumnIndexByColumnName(fetchResultSet, sourceColumn.toUpperCase());
                            int columnType = fetchResultSet.getMetaData().getColumnType(columnIndex);
                            JDBCStorage jdbcSourceStorage = chunk.getSourceStorage().unwrap(JDBCStorage.class);
                            switch (columnType) {
                                // INTERVALYM
                                case -103:
                                    Serializable intervalym = (Serializable) fetchResultSet.getObject(sourceColumn);
                                    interval = ColumnUtil.byteArrayYMToInterval(jdbcSourceStorage.intervalYM2Interval(intervalym));
                                    break;
                                // INTERVALDS
                                case -104:
                                    Serializable intervalds = (Serializable) fetchResultSet.getObject(sourceColumn);
                                    interval = ColumnUtil.byteArrayDSToInterval(jdbcSourceStorage.intervalDS2Interval(intervalds));
                                    break;
                                default:
                                    break;
                            }
                        } else if (chunk instanceof PGChunk<?, ?, ?, ?>) {
                            PGInterval pgInterval = (PGInterval) fetchResultSet.getObject(sourceColumn);
                            interval = new Interval(
                                    pgInterval.getYears() * 12 + pgInterval.getMonths(),
                                    pgInterval.getDays(),
                                    pgInterval.getHours(),
                                    pgInterval.getMinutes(),
                                    (int) pgInterval.getSeconds(),
                                    pgInterval.getMicroSeconds());
                        }
                        row.setInterval(targetColumn, interval);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                case "bytea": {
                    try {
                        Object o = fetchResultSet.getObject(sourceColumn);
                        if (o == null) {
                            row.setByteArray(targetColumn, null);
                            break;
                        }
                        byte[] bytes = new byte[0];
                        if (chunk.getSourceStorage().getClass().getName().equals(ORACLE_STORAGE_CLASS_NAME)) {
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
                        } else if (chunk instanceof PGChunk<?, ?, ?, ?>) {
                            bytes = fetchResultSet.getBytes(sourceColumn);
                        }
                        row.setByteArray(targetColumn, bytes);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "bool": {
                    try {
                        Object o = fetchResultSet.getObject(sourceColumn);
                        if (o == null) {
                            row.setBoolean(targetColumn, null);
                            break;
                        }
                        boolean b = fetchResultSet.getBoolean(sourceColumn);
                        row.setBoolean(targetColumn, b);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "inet":
                    try {
                        Object o = fetchResultSet.getObject(sourceColumn);
                        if (o == null) {
                            row.setInet4Addr(targetColumn, null);
                            break;
                        }
                        try {
                            InetAddress inetAddress = InetAddress.getByName(fetchResultSet.getString(sourceColumn));
                            if (inetAddress instanceof Inet4Address inet4Address) {
                                row.setInet4Addr(targetColumn, inet4Address);
                            } else {
                                Inet6Address inet6Address = (Inet6Address) inetAddress;
                                row.setInet6Addr(targetColumn, inet6Address);
                            }
                        } catch (UnknownHostException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
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
                            try {
                                uuid = UUID.fromString((String) o);
                            } catch (Exception e1) {
                                log.error("{}.{} : {} {} {}", chunk.getTargetTable().getSchemaName(),
                                        chunk.getTargetTable().getTableName(), targetColumn, o, getStackTrace(e1));
                            }
                        }
                        row.setUUID(targetColumn, uuid);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                case "_uuid":
                    try {
                        Object o = fetchResultSet.getObject(sourceColumn);
                        if (o == null) {
                            row.setUUIDArray(targetColumn, null);
                            break;
                        }
                        List<UUID> arr = List.of(((UUID[]) fetchResultSet.getArray(sourceColumn).getArray()));
                        row.setUUIDArray(targetColumn, arr);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                default:
                    try {
                        if (chunk.getConfig().tryCharIfAny() != null) {
                            if (chunk.getConfig().tryCharIfAny().contains(targetColumn)) {
                                String s = fetchResultSet.getString(sourceColumn);
                                if (s == null) {
                                    row.setText(targetColumn, null);
                                    break;
                                }
                                row.setText(targetColumn, s.replaceAll("\u0000", ""));
                                break;
                            } else {
                                log.error("There is no handler for type: {}  for column: {}", targetType, targetColumn);
                                writer.close();
                                connectionTo.close();
                            }
                        } else {
                            log.error("tryCharIfAny is NULL for Table: {}.{} Column: {} Type: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(),
                                    targetType, targetColumn);
                            throw new RuntimeException("Unsupported type: " + targetType);
                        }
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("Table: {}.{} Column: {} Type: {}: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(),
                                targetType, targetColumn, getStackTrace(e));
                        throw e;
                    }
            }
        }
    }

    @Override
    public String buildFetchStatement(Config config) {
        return buildFetchStatement(config, null);
    }

    @Override
    public String buildFetchStatement(Config config, Chunk<K, T, S, R> chunk) {
        List<String> strings = new ArrayList<>();
        Map<String, String> columnToColumnMap = config.columnToColumn();
        if (chunk != null && columnToColumnMap == null) {
            Table<?> sourceTable = chunk.getSourceTable();
            strings.addAll(
                    sourceTable.getColumns()
                            .stream()
                            .map(Column::columnName)
                            .toList()
            );
        } else if (columnToColumnMap != null) {
            strings.addAll(columnToColumnMap.keySet());
        }
        Map<String, String> expressionToColumnMap = config.expressionToColumn();
        if (expressionToColumnMap != null) {
            strings.addAll(expressionToColumnMap.keySet());
        }
        String columnToColumn = String.join(", ", strings);
        return PGKeywords.SELECT + " " +
                columnToColumn + " " +
                PGKeywords.FROM + " " +
                config.fromSchemaName() +
                "." +
                config.fromTableName() + " " +
                (config.fromTableAlias() == null ? "" : config.fromTableAlias()) + " " +
                (config.fromTableAdds() == null ? "" : config.fromTableAdds()) + " " +
                PGKeywords.WHERE + " " +
                (config.fetchWhereClause() == null ? "" : " ( " + config.fetchWhereClause() + " ) and ") + " " +
                (config.fromTableAlias() == null ? "" : config.fromTableAlias() + ".") +
                "ctid >= concat('(', ? ,',1)')::tid and " +
                (config.fromTableAlias() == null ? "" : config.fromTableAlias() + ".") +
                "ctid < concat('(', ? ,',1)')::tid";
    }

    @Override
    public void createPrimaryKeys() {
        Map<Table<S>, Table<S>> tables = getTables();
        try {
            Connection targetConnection = getPoolConnection();
            for (Map.Entry<Table<S>, Table<S>> entry : tables.entrySet()) {
                Table<S> targetTable = entry.getValue();
                targetTable.createPrimaryKey(targetConnection);
            }
            targetConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void createIndexes() {
        Map<Table<S>, Table<S>> tables = getTables();
        try {
            Connection targetConnection = getPoolConnection();
            for (Map.Entry<Table<S>, Table<S>> entry : tables.entrySet()) {
                Table<S> taregtTable = entry.getValue();
                taregtTable.createIndexes(targetConnection);
            }
            targetConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void createTables() {
        Map<Table<S>, Table<S>> tables = getTables();
        try {
            Connection targetConnection = getPoolConnection();
            for (Map.Entry<Table<S>, Table<S>> entry : tables.entrySet()) {
                Table<S> taregtTable = entry.getValue();
                taregtTable.create(targetConnection);
            }
            targetConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void createForeignKeys() {
        Map<Table<S>, Table<S>> tables = getTables();
        try {
            Connection targetConnection = getPoolConnection();
            for (Map.Entry<Table<S>, Table<S>> entry : tables.entrySet()) {
                Table<S> taregtTable = entry.getValue();
                taregtTable.createForeignKeys(targetConnection);
            }
            targetConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public <W extends Serializable> byte[] intervalYM2Interval(W intervalym) {
        return null;
    }

    @Override
    public <W extends Serializable> byte[] intervalDS2Interval(W intervalds) {
        return null;
    }

    @Override
    public void createUniqueConstraints() {
        Map<Table<S>, Table<S>> tables = getTables();
        try {
            Connection targetConnection = getPoolConnection();
            for (Map.Entry<Table<S>, Table<S>> entry : tables.entrySet()) {
                Table<S> taregtTable = entry.getValue();
                taregtTable.createUniqueConstraints(targetConnection);
            }
            targetConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void enrichSourceTables(Connection connection) {
        Map<Table<S>, Table<S>> tables = getTables();
        try {
            for (Map.Entry<Table<S>, Table<S>> entry : tables.entrySet()) {
                Table<S> sourceTable = entry.getKey();
                List<Column> allSourceColumns = sourceTable.getAllColumns((S)connection);
                sourceTable.setColumns(allSourceColumns);

                List<Column> sourcePKColumns = sourceTable.getPrimaryKeyColumns(connection);
                sourceTable.setPkColumns(sourcePKColumns);

                if (getMajorStorageVersion(connection) >= 15) {
                    List<UniqueConstraint> uniqueConstraints = sourceTable.getUniqueConstraints(connection);
                    sourceTable.setUniqueConstraints(uniqueConstraints);
                }

/*
                List<Index> sourceIndexes = sourceTable.getTableIndexes(connection);
                sourceTable.setIndexes(sourceIndexes);
*/

                List<ForeignKey> foreignKeys = sourceTable.getForeignKeys(connection, this, entry.getValue());
                sourceTable.setForeignKeys(foreignKeys);

                Map.Entry<Integer, List<TableOption>> options = sourceTable.getOptions(connection);
                sourceTable.setId(options.getKey());
                sourceTable.setOptions(options.getValue());
            }
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public Map.Entry<String,Long> getSystemChangeNumberWithTrxId() throws SQLException {
        try (Statement st = getPoolConnection().createStatement();
             ResultSet rs = st.executeQuery(SQL_PG_CURRENT_LSN_AND_XID)) {
            if (rs.next()) {
                String lsn = rs.getString(1);
                Long xid = rs.getLong(2);
                return Map.entry(LogSequenceNumber.valueOf(lsn).asString(), xid);
            } else {
                return Map.entry(LogSequenceNumber.INVALID_LSN.asString(), 0L);
            }
        }
    }

    @Override
    public void createChunks(List<Config> configs,
                             boolean sync,
                             int required,
                             String tableName) throws SQLException {
        Connection connection = getConnection();
        createChunkTable(connection, sync, tableName);
        for (Config config : configs) {
            long reltuples = 0;
            long relpages = 0;
            long max_end_page;
            Table table = configToTable(config.fromSchemaName(), config.fromTableName());

            PreparedStatement preparedStatement = connection.prepareStatement(SQL_NUMBER_OF_TUPLES);
            preparedStatement.setString(1, table.getSchemaName().toLowerCase());
            preparedStatement.setString(2, table.getFinalTableName(false));
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                reltuples = resultSet.getLong("reltuples");
                relpages = resultSet.getLong("relpages");
            }
            resultSet.close();
            preparedStatement.close();

            long heap_blks_total = getTotalPagesOfTable(connection, table);
            long v = reltuples <= 0 && relpages <= 1 ? relpages + 1 :
                    (int) Math.round(relpages / (reltuples / (double) required));
            long pagesInChunk = Math.min(v, relpages + 1);
            log.debug("{}.{} \t\t\t relpages : {}\t heap_blks_total : {}\t reltuples : {}\t rowsInChunk : {}\t pagesInChunk : {} ",
                    config.fromSchemaName(),
                    config.fromTableName(),
                    relpages,
                    heap_blks_total,
                    reltuples,
                    (double) required,
                    pagesInChunk);
            insertCtidChunksV2(connection, config, table, 0, relpages, pagesInChunk, ChunkStatus.UNASSIGNED, required, tableName);

            max_end_page = getMaxEndPageOfChunks(connection, config, tableName);

            // всавка последних чанков
            if (heap_blks_total > max_end_page) {
                insertCtidChunksV2(connection, config, table, max_end_page, heap_blks_total, pagesInChunk, ChunkStatus.UNASSIGNED, required, tableName);
            }
        }
        if (!sync) {
            connection.commit();
        }
        log.info("Ctid chunks created successfully");
    }

    @Override
    public void createOutbox(String tableName) throws SQLException {
        Connection connection = getPoolConnection();
        try {
            Statement createTable = connection.createStatement();
            createTable.executeUpdate(DDL_CREATE_OUTBOX_TABLE.replace("$tableName", tableName));
            createTable.close();
            connection.commit();
            log.info("Outbox table created successfully");
        } catch (SQLException e) {
            log.warn("Outbox table already exists");
//            log.warn("{}", getStackTrace(e));
        }
        connection.close();
    }

    private void createChunkTable(Connection connection, boolean sync, String chunkTableName) throws SQLException {
        try {
            Statement createTable = connection.createStatement();
            createTable.executeUpdate(DDL_CREATE_CHUNK_TABLE.replace("$tableName", chunkTableName));
            createTable.close();
            if (!sync) {
                connection.commit();
            }
        } catch (SQLException e) {
//            log.error("{}", getStackTrace(e));
            throw e;
        }
    }

    @Override
    public void dropChunkTable(boolean sync, String tableName) {
        Connection connection = getConnection();
        try (Statement dropTable = connection.createStatement()) {
            dropTable.executeUpdate(DDL_DROP_CHUNK_TABLE.replace("$tableName", tableName));
            dropTable.close();
            connection.commit();
            if (!sync) {
                connection.commit();
            }
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            log.warn("Chunk table {} does not exist", tableName);
//            log.warn("{}", getStackTrace(e));
        }
    }

    @Override
    public void dropOutboxTable(boolean sync, String tableName) throws SQLException {
        Connection connection = getPoolConnection();
        try (Statement dropTable = connection.createStatement()){
            dropTable.executeUpdate(DDL_DROP_OUTBOX_TABLE.replace("$tableName", tableName));
            dropTable.close();
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            log.warn("Outbox table {} does not exist", tableName);
//            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void insertProcessedChunkInfo(Connection connection, int chunkId, int rows, String taskName, String tableName) throws SQLException {
        PreparedStatement chunkInsert = connection.prepareStatement(DML_INSERT_OUTBOX_TABLE.replace("$tableName", tableName));
        chunkInsert.setLong(1, chunkId);
        chunkInsert.setString(2, taskName);
        chunkInsert.setLong(3, rows);
        long r = chunkInsert.executeUpdate();
        chunkInsert.close();
    }


    @Override
    public Table configToTable(String schemaName, String tableName) {
        return new PGTable(schemaName, tableName);
    }
}
