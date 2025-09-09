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
import org.bublik.core.service.JDBCStorageService;
import org.bublik.core.storage.JDBCStorage;
import org.bublik.core.storage.StorageClass;
import org.bublik.postgres.model.PGChunk;
import org.bublik.postgres.model.PGTable;
import org.bublik.postgres.util.ColumnUtil;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.util.*;
import java.util.function.Consumer;

import static org.bublik.core.constants.CLassConstants.ORACLE_STORAGE_CLASS_NAME;
import static org.bublik.core.constants.SQLConstants.*;
import static org.bublik.core.util.ColumnUtil.*;
import static org.bublik.core.util.Utils.getStackTrace;

public class JDBCPostgreSQLStorage extends JDBCStorage implements JDBCStorageService {
    private static final Logger log = LoggerFactory.getLogger(JDBCPostgreSQLStorage.class);

    public JDBCPostgreSQLStorage(StorageClass storageClass, ConnectionProperty connectionProperty) throws SQLException {
        super(storageClass, connectionProperty);
    }

    @Override
    public Map<Integer, Chunk<?>> getChunkMap(List<Config> configs, Connection connection) throws SQLException {
        Map<Integer, Chunk<?>> chunkHashMap = new TreeMap<>();
        String sql = buildStartEndOfChunk(configs);
        log.debug("SQL to fetch metadata of chunks: \n{}", sql);
//        StringBuffer sb = new StringBuffer();
        Map<String, Table> tableMap = new HashMap<>();
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.isBeforeFirst()) {
            while (resultSet.next()) {
                Config config = findByTaskName(configs, resultSet.getString("task_name"));
//                log.info("{}", getTables().size());
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
                String query;
                if (config.columnToColumn() == null && config.expressionToColumn() == null) {
                    query = buildFetchStatement(config, sourceTable);
                } else {
                    query = buildFetchStatement(config);
                }
                tableMap.put(query, sourceTable);
                chunkHashMap.put(resultSet.getInt("rownum"),
                        new PGChunk<>(
                                resultSet.getInt("chunk_id"),
                                resultSet.getLong("start_page"),
                                resultSet.getLong("end_page"),
                                config,
                                sourceTable,
                                query,
                                this
                        )
                );
            }
        }
        tableMap.keySet().forEach(s -> log.info("{}", s));
        resultSet.close();
        statement.close();
        return chunkHashMap;
    }

    @Override
    public String buildStartEndOfChunk(List<Config> configs) {
        List<String> taskNames = new ArrayList<>();
        configs.forEach(sqlStatement -> taskNames.add(sqlStatement.fromTaskName()));
        return "select row_number() over (order by chunk_id) as rownum, chunk_id, start_page, end_page, task_name from public.ctid_chunks where task_name in ('" +
                String.join("', '", taskNames) + "') " +
                // тут надо разбираться при запуске из нескольких подов
                "and status in ('ASSIGNED', 'UNASSIGNED', 'PROCESSED_WITH_ERROR') ";
    }

    @Override
    public LogMessage transferToTarget(Chunk<?> chunk) throws SQLException, BinaryWriteFailedException,
            SourceSQLException, TargetSQLException {
        ResultSet fetchResultSet = chunk.getResultSet();
        Connection connectionFrom = chunk.getSourceConnection();
        if (fetchResultSet.next()) {
            Connection connectionTo;
            try {
                connectionTo = getConnection();
//                PGConnection connection = connectionTo.unwrap(PGConnection.class);
            } catch (SQLTransientConnectionException t) {
                throw new TargetSQLException(getStackTrace(t));
            }
            chunk.setTargetConnection(connectionTo);
//            Table table = TableService.getTable(connectionTo, chunk.getConfig().toSchemaName(), chunk.getConfig().toTableName());
            Table table = configToTable(chunk.getConfig().toSchemaName(), chunk.getConfig().toTableName());
            chunk.setTargetTable(table);
            try {
                LogMessage logMessage = fetchAndCopy(connectionTo, fetchResultSet, chunk);
                connectionTo.close();
                return logMessage;
            } catch (SQLException e) {
//                log.error("{}", getStackTrace(e));
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
            return new LogMessage(
                    0,
                    chunk.getStartTime(),
                    System.currentTimeMillis(),
                    "NO ROWS FETCH",
                    chunk);
        }
    }

    private LogMessage fetchAndCopy(Connection connectionTo,
                                    ResultSet fetchResultSet,
                                    Chunk<?> chunk) throws SQLException, BinaryWriteFailedException, SourceSQLException{
        int recordCount = 0;

        try {
            chunk.insertProcessedChunkInfo(connectionTo, recordCount);
            connectionTo.rollback();
        } catch (PSQLException p) {
            connectionTo.rollback();
            return new LogMessage(
                    0,
                    chunk.getStartTime(),
                    System.currentTimeMillis(),
                    "The chunk has already been copied",
                    chunk);
        }

        Map<String, Column> columnToColumnMap = readTargetColumnsAndTypes(connectionTo, chunk);
//        columnToColumnMap.forEach((k, v) -> log.info("Column to copy: {} -> {}:{}", k, v.getColumnName(), v.getColumnType()));
//        neededColumnsToDB.forEach((s, pgColumn) -> System.out.println(s + " " + pgColumn.getColumnName() + ":" + pgColumn.getColumnType()));
        Map<List<String>, Column> neededColumnsFromMany = readTargetColumnsAndTypesFromMany(connectionTo, chunk);

//        Map<String, PGEncryptedColumn> neededEncryptedColumns = readTargetEncryptedColumnsAndTypes(connectionTo, chunk);
/*
        neededEncryptedColumns.forEach((s1, pgEncryptedColumn) -> System.out.println(s1 + " " +
                pgEncryptedColumn.column().getColumnName() + " " +
                pgEncryptedColumn.encryptedColumn().targetEncColumnName() + " " +
                pgEncryptedColumn.encryptedColumn().targetEncMetaColumnName()));
*/
        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connectionTo);

        String[] columnNames = columnToColumnMap
                .values()
                .stream()
                .map(Column::getColumnName)
                .toList()
                .toArray(String[]::new);
/*
        String[] metaColumnNames = neededEncryptedColumns
                .values()
                .stream()
                .map(PGEncryptedColumn::encryptedColumn)
                .toList()
                .stream().map(EncryptedColumn::targetEncMetaColumnName)
                .filter(Objects::nonNull)
                .toArray(String[]::new);
*/
        String[] cNames = Arrays.copyOf(columnNames, columnNames.length);
//        log.info("Here... {}", columnNames.length);
//        Arrays.stream(cNames).forEach(c -> log.info("Column for COPY: {}", c));
//        String[] cNames = Arrays.copyOf(columnNames, columnNames.length + metaColumnNames.length);
//        System.arraycopy(metaColumnNames, 0, cNames, columnNames.length, metaColumnNames.length);
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
        chunk.insertProcessedChunkInfo(connectionTo, recordCount);
        connectionTo.commit();

        return new LogMessage(
                recordCount,
                chunk.getStartTime(),
                System.currentTimeMillis(),
                "PostgreSQL COPY",
                chunk);
    }

    private boolean hasNext(ResultSet resultSet) throws SourceSQLException {
        try {
            return resultSet.next();
        } catch (SQLException e) {
//            LOGGER.error("{}", getStackTrace(e));
            throw new SourceSQLException(getStackTrace(e));
        }
    }

    @Override
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?> chunk) {
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
            Map<String, EncryptedColumn> encryptedEntityMap = chunk.getConfig().expressionToCrypto();
            Map<String, String> cryptoToColumnMap = chunk.getConfig().cryptoToColumn();

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
                    Table sourceTable = chunk.getSourceTable();
                    sourceTable.getColumns().forEach(column -> columnMap.put(column.getColumnName(), column));
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

                if (encryptedEntityMap != null) {
                    encryptedEntityMap
                            .entrySet()
                            .stream()
                            .filter(s -> {
                                if (s.getValue().targetEncColumnName() != null) {
                                    return s.getValue().targetEncColumnName().replaceAll("\"", "").equalsIgnoreCase(columnName);
                                }
                                return false;
                            })
                            .forEach(i -> columnMap.put(columnName,
                                    new Column(
                                            columnPosition,
                                            i.getValue().targetEncColumnName(),
                                            columnType.equals("bigserial") ? "bigint" : columnType,
                                            dataType, null, null, null, null, 0 , null, 0, null)));
                }

                if (cryptoToColumnMap != null) {
                    cryptoToColumnMap
                            .entrySet()
                            .stream()
                            .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(columnName))
                            .forEach(i -> columnMap.put(i.getKey(),
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

    protected Map<List<String>, Column> readTargetColumnsAndTypesFromMany(Connection connectionTo, Chunk<?> chunk) {
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

    protected Map<String, PGEncryptedColumn> readTargetEncryptedColumnsAndTypes(Connection connectionTo, Chunk<?> chunk) {
        Map<String, PGEncryptedColumn> columnMap = new HashMap<>();
        try {
            ResultSet resultSet = connectionTo.getMetaData().getColumns(
                    null,
                    chunk.getTargetTable().getSchemaName().toLowerCase(),
                    chunk.getTargetTable().getFinalTableName(false),
                    null);
            Map<String, EncryptedColumn> encryptedEntityMap = chunk.getConfig().expressionToCrypto();

            while (resultSet.next()) {
                String columnName = resultSet.getString(4);
                String columnType = resultSet.getString(6);
                Integer columnPosition = resultSet.getInt(17);

                if (encryptedEntityMap != null) {
                    encryptedEntityMap
                            .entrySet()
                            .stream()
                            .filter(entry -> {
                                if (entry.getValue().targetEncColumnName() != null) {
                                    return entry.getValue().targetEncColumnName().replaceAll("\"", "").equalsIgnoreCase(columnName);
                                } else return entry.getValue().targetEncMetaColumnName() != null;
                            })
                            .forEach(entry -> columnMap.put(
                                columnName,
                                    new PGEncryptedColumn(
                                        new Column(
                                                columnPosition,
                                                entry.getValue().targetEncColumnName() == null ? entry.getValue().targetEncMetaColumnName() : entry.getValue().targetEncColumnName(),
                                                columnType.equals("bigserial") ? "bigint" : columnType,
                                                null, null, null, null, null, 0 , null, 0, null),
                                        entry.getValue()
                            )));
                }

            }
            resultSet.close();
        } catch (SQLException e) {
            log.error("{}", e.getMessage());
        }
        return columnMap;
    }

    private void simpleRowConsume(SimpleRow row,
                                  Map<String, Column> neededColumnsToDB,
//                                  Map<String, PGEncryptedColumn> neededEncryptedColumns,
                                  Map<List<String>, Column> neededColumnsFromMany,
                                  ResultSet fetchResultSet,
                                  Chunk<?> chunk,
                                  Connection connectionTo,
                                  SimpleRowWriter writer) throws SQLException, BinaryWriteFailedException {
        for (Map.Entry<String, Column> entry : neededColumnsToDB.entrySet()) {
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
                        } else if (chunk instanceof PGChunk<?>) {
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
                        log.error("\u001B[31m{}.{} {} -> {}\u001B[0m: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
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
                        log.error("\u001B[31m{}.{} {} -> {}\u001B[0m: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
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
                        log.error("\u001B[31m{}.{} {} -> {}\u001B[0m: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
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
                        log.error("\u001B[31m{}.{} {} -> {}\u001B[0m: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
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
                                .filter(i -> i.getValue().getColumnName().equals(targetColumn))
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
                        log.error("\u001B[31mtstzrange\u001B[0m : {}.{} - {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
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
                        } else if (chunk instanceof PGChunk<?>) {
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
                        } else if (chunk instanceof PGChunk<?>) {
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
                            log.error("tryCharIfAny is NULL for type: {}  for column: {}", targetType, targetColumn);
                            throw new RuntimeException();
                        }
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
            }
        }
    }

    @Override
    public String buildFetchStatement(Config config) {
        return buildFetchStatement(config, null);
    }

    public String buildFetchStatement(Config config, Table sourceTable) {
        List<String> strings = new ArrayList<>();
        Map<String, String> columnToColumnMap = config.columnToColumn();
        if (sourceTable != null && columnToColumnMap == null) {
            strings.addAll(
                    sourceTable.getColumns()
                            .stream()
                            .map(Column::getColumnName)
                            .toList()
            );
        } else if (columnToColumnMap != null) {
            strings.addAll(columnToColumnMap.keySet());
        }
        Map<String, String> expressionToColumnMap = config.expressionToColumn();
        Map<String, EncryptedColumn> encryptedEntityMap = config.expressionToCrypto();
        Map<String, String> cryptoToColumnMap = config.cryptoToColumn();
        if (expressionToColumnMap != null) {
            strings.addAll(expressionToColumnMap.keySet());
        }
        if (encryptedEntityMap != null) {
            strings.addAll(encryptedEntityMap.keySet());
        }
        if (cryptoToColumnMap != null) {
            strings.addAll(cryptoToColumnMap.keySet());
        }
        String columnToColumn = String.join(", ", strings);
        return PGKeywords.SELECT + " " +
                columnToColumn + " " +
//                "xmax::text::xid8) as xmax_status " +
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
        Map<Table, Table> tables = getTables();
        try {
            Connection targetConnection = getConnection();
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table targetTable = entry.getValue();
                targetTable.createPrimaryKey(targetConnection);
            }
            targetConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void createIndexes() {
        Map<Table, Table> tables = getTables();
        try {
            Connection targetConnection = getConnection();
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table taregtTable = entry.getValue();
                taregtTable.createIndexes(targetConnection);
            }
            targetConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void createTables() {
        Map<Table, Table> tables = getTables();
        try {
            Connection targetConnection = getConnection();
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table taregtTable = entry.getValue();
                taregtTable.create(targetConnection);
            }
            targetConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void createForeignKeys() {
        Map<Table, Table> tables = getTables();
        try {
            Connection targetConnection = getConnection();
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table taregtTable = entry.getValue();
                taregtTable.createForeignKeys(targetConnection);
            }
            targetConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public <T extends Serializable> byte[] intervalYM2Interval(T intervalym) {
        return null;
    }

    @Override
    public <T extends Serializable> byte[] intervalDS2Interval(T intervalds) {
        return null;
    }

    @Override
    public void createUniqueConstraints() {
        Map<Table, Table> tables = getTables();
        try {
            Connection targetConnection = getConnection();
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table taregtTable = entry.getValue();
                taregtTable.createUniqueConstraints(targetConnection);
            }
            targetConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void enrichSourceTables(Connection connection, Map<Table, Table> tables) {
        try {
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table sourceTable = entry.getKey();
                List<Column> allSourceColumns = sourceTable.getAllColumns(connection);
                List<Column> sourcePKColumns = sourceTable.getPrimaryKeyColumns(connection);
                if (getMajorStorageVersion(connection) >= 14) {
                    List<UniqueConstraint> uniqueConstraints = sourceTable.getUniqueConstraints(connection);
                    sourceTable.setUniqueConstraints(uniqueConstraints);
                }
                List<Index> sourceIndexes = sourceTable.getTableIndexes(connection);
                List<ForeignKey> foreignKeys = sourceTable.getForeignKeys(connection, this, entry.getValue());
                Map.Entry<Integer, List<TableOption>> options = sourceTable.getOptions(connection);

                sourceTable.setId(options.getKey());
                sourceTable.setOptions(options.getValue());
                sourceTable.setColumns(allSourceColumns);
                sourceTable.setPkColumns(sourcePKColumns);
                sourceTable.setIndexes(sourceIndexes);
                sourceTable.setForeignKeys(foreignKeys);
            }
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public Map.Entry<String,Long> getSystemChangeNumberWithTrxId() throws SQLException {
        try (Statement st = getConnection().createStatement();
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
    public void createChunks(Connection connection, List<Config> configs, boolean sync, int required) throws SQLException {
        createTableCtidChunks(connection, sync);
        try {
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
                insertCtidChunksV2(connection, config, table, 0, relpages, pagesInChunk, ChunkStatus.UNASSIGNED, required, 0, 0);

                max_end_page = getMaxEndPageOfChunks(connection, config);

                // всавка последних чанков
                if (heap_blks_total > max_end_page) {
                    insertCtidChunksV2(connection, config, table, max_end_page, heap_blks_total, pagesInChunk, ChunkStatus.UNASSIGNED, required, 0, 0);
                }
            }
            if (!sync) {
                connection.commit();
            }
            log.info("Ctid chunks created successfully");
        } catch (SQLException e) {
            log.warn("{}", getStackTrace(e));
        }
    }

    @Override
    public void createOutbox() throws SQLException {
        Connection connection = getConnection();
        try {
            Statement createTable = connection.createStatement();
            createTable.executeUpdate(DDL_CREATE_PG_TABLE_BUBLIK_OUTBOX);
            createTable.close();
            Statement truncateTable = connection.createStatement();
            truncateTable.executeUpdate(DDL_TRUNCATE_PG_TABLE_BUBLIK_OUTBOX);
            truncateTable.close();
            connection.commit();
            log.info("Outbox table created successfully");
        } catch (SQLException e) {
            log.warn("{}", getStackTrace(e));
        }
        connection.close();
    }

    private void createTableCtidChunks(Connection connection, boolean sync) {
        try {
            try {
                Statement dropTable = connection.createStatement();
                dropTable.executeUpdate(DDL_DROP_PG_TABLE_CTID_CHUNKS);
                dropTable.close();
                connection.commit();
            } catch (SQLException ex) {
                connection.rollback();
                log.error("Error dropping table ctid_chunks, it may not exist yet.");
            }
            Statement createTable = connection.createStatement();
            createTable.executeUpdate(DDL_CREATE_PG_TABLE_CTID_CHUNKS);
            createTable.close();
            Statement truncateTable = connection.createStatement();
            truncateTable.executeUpdate(DDL_TRUNCATE_PG_TABLE_CTID_CHUNKS);
            truncateTable.close();
            if (!sync) {
                connection.commit();
            }
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }


    @Override
    public Table configToTable(String schemaName, String tableName) {
        return new PGTable(schemaName, tableName);
    }
}
