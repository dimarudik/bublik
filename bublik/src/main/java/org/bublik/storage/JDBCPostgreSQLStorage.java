package org.bublik.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.bytefish.pgbulkinsert.exceptions.BinaryWriteFailedException;
import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.pgsql.model.interval.Interval;
import de.bytefish.pgbulkinsert.pgsql.model.range.Range;
import de.bytefish.pgbulkinsert.row.SimpleRow;
import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import org.bublik.constants.ChunkStatus;
import org.bublik.constants.PGKeywords;
import org.bublik.exception.SourceSQLException;
import org.bublik.exception.TableNotExistsException;
import org.bublik.exception.TargetSQLException;
import org.bublik.model.*;
import org.bublik.service.JDBCStorageService;
import org.bublik.service.StorageService;
import org.bublik.service.Syncable;
import org.bublik.service.TableService;
import org.postgresql.PGConnection;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static org.bublik.constants.SQLConstants.*;
import static org.bublik.exception.Utils.getStackTrace;
import static org.bublik.util.ColumnUtil.*;

public class JDBCPostgreSQLStorage extends JDBCStorage implements JDBCStorageService, Syncable {
    private static final Logger log = LoggerFactory.getLogger(JDBCPostgreSQLStorage.class);
    private static JDBCPostgreSQLStorage toInstance;
    private static JDBCPostgreSQLStorage fromInstance;

    private JDBCPostgreSQLStorage(StorageClass storageClass,
                                  ConnectionProperty connectionProperty,
                                  Boolean isSource) throws SQLException {
        super(storageClass, connectionProperty, isSource);
    }

    public static synchronized JDBCPostgreSQLStorage getInstance(StorageClass storageClass,
                                                                 ConnectionProperty connectionProperty,
                                                                 Boolean isSource) throws SQLException{
        try {
            if (isSource) {
                if (toInstance == null) {
                    toInstance = new JDBCPostgreSQLStorage(storageClass, connectionProperty, isSource);
                }
                return toInstance;
            }
            if (fromInstance == null) {
                fromInstance = new JDBCPostgreSQLStorage(storageClass, connectionProperty, isSource);
            }
            return fromInstance;
        } catch (Exception e) {
            log.error("Connection error: {}", getStackTrace(e));
            throw e;
        }
    }

    @Override
    public Map<Integer, Chunk<?>> getChunkMap(List<Config> configs) throws SQLException {
        Map<Integer, Chunk<?>> chunkHashMap = new TreeMap<>();
//        Map<Integer, Chunk<?>> chunkHashMap = new HashMap<>();
        String sql = buildStartEndOfChunk(configs);
        log.debug("SQL to fetch metadata of chunks: \n{}", sql);
        StringBuffer sb = new StringBuffer();
        for (Config c : configs)
            sb.append("\n").append(buildFetchStatement(c));
        log.debug("SQL to fetch chunks: {}", sb);
        Connection initialConnection = getConnection();
        PreparedStatement statement = initialConnection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.isBeforeFirst()) {
            while (resultSet.next()) {
//                log.info("Fetched {} chunks from PostgreSQL", chunkHashMap.size());
                Config config = findByTaskName(configs, resultSet.getString("task_name"));
                Table sourceTable = TableService.getTable(initialConnection, config.fromSchemaName(), config.fromTableName());
/*
                if (!sourceTable.exists(initialConnection)) {
                    initialConnection.close();
                    log.error("\u001B[31mThe Source Table: {}.{} does not exist.\u001B[0m", sourceTable.getSchemaName(),
                            sourceTable.getTableName());
                    throw new TableNotExistsException(sourceTable.getSchemaName(), sourceTable.getTableName());
                }
*/
                String query = buildFetchStatement(config);
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
        resultSet.close();
        statement.close();
        initialConnection.close();
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
            } catch (SQLTransientConnectionException t) {
                throw new TargetSQLException(getStackTrace(t));
            }
            chunk.setTargetConnection(connectionTo);
            Table table = TableService.getTable(connectionTo, chunk.getConfig().toSchemaName(), chunk.getConfig().toTableName());
            if (table.exists(connectionTo)) {
                chunk.setTargetTable(table);
                try {
                    LogMessage logMessage = fetchAndCopy(connectionTo, fetchResultSet, chunk);
                    connectionTo.close();
                    return logMessage;
                } catch (SQLException e) {
                    log.error("{}", getStackTrace(e));
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
                log.error("\u001B[31mThe Target Table: {}.{} does not exist.\u001B[0m", chunk.getConfig().toSchemaName(),
                        chunk.getConfig().toTableName());
                throw new TableNotExistsException("The Target Table "
                        + chunk.getConfig().toSchemaName() + "."
                        + chunk.getConfig().toTableName() + " does not exist.");
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

        Map<String, Column> neededColumnsToDB = readTargetColumnsAndTypes(connectionTo, chunk);
//        neededColumnsToDB.forEach((s, pgColumn) -> System.out.println(s + " " + pgColumn.getColumnName() + ":" + pgColumn.getColumnType()));
        Map<List<String>, Column> neededColumnsFromMany = readTargetColumnsAndTypesFromMany(connectionTo, chunk);

        Map<String, PGEncryptedColumn> neededEncryptedColumns = readTargetEncryptedColumnsAndTypes(connectionTo, chunk);
        neededEncryptedColumns.forEach((s1, pgEncryptedColumn) -> System.out.println(s1 + " " +
                pgEncryptedColumn.column().getColumnName() + " " +
                pgEncryptedColumn.encryptedColumn().targetEncColumnName() + " " +
                pgEncryptedColumn.encryptedColumn().targetEncMetaColumnName()));
        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connectionTo);

        String[] columnNames = neededColumnsToDB
                .values()
                .stream()
                .map(Column::getColumnName)
                .toList()
                .toArray(String[]::new);
        String[] metaColumnNames = neededEncryptedColumns
                .values()
                .stream()
                .map(PGEncryptedColumn::encryptedColumn)
                .toList()
                .stream().map(EncryptedColumn::targetEncMetaColumnName)
                .filter(Objects::nonNull)
                .toArray(String[]::new);
        String[] cNames = Arrays.copyOf(columnNames, columnNames.length + metaColumnNames.length);
        System.arraycopy(metaColumnNames, 0, cNames, columnNames.length, metaColumnNames.length);
        SimpleRowWriter.Table table =
                new SimpleRowWriter.Table(chunk.getTargetTable().getSchemaName(),
                        chunk.getTargetTable().getFinalTableName(true), cNames);

        SimpleRowWriter writer = new SimpleRowWriter(table, pgConnection);
        Consumer<SimpleRow> simpleRowConsumer =
            s -> {
                try {
                    simpleRowConsume(s, neededColumnsToDB, neededColumnsFromMany,
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
                                            dataType, null, null, null, null)));
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
                                            dataType, null, null, null, null)));
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
                                            dataType, null, null, null, null)));
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
                                            dataType, null, null, null, null)));
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
                                            dataType, null, null, null, null)));
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
                                            dataType, null, null, null, null)));
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
                                                null, null, null, null, null),
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
//        System.out.println("simpleRowConsume...");
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
                            row.setText(targetColumn, null);
                            break;
                        }
/*
                        if (neededEncryptedColumns.get(targetColumn) != null) {
                            String aad = fetchResultSet.getObject(neededEncryptedColumns.get(targetColumn).encryptedColumn().sourceAadColumnName()).toString();
                            System.out.println(aad);
                            EncryptedEntity encryptedEntity = SecureUtil.getEncryptedEntity(getConnectionProperty(), s, aad);
                            String e = encryptedEntity.obtainEncryptedData();
                            if (neededEncryptedColumns.get(targetColumn).encryptedColumn().targetEncColumnName() != null) {
                                row.setText(neededEncryptedColumns.get(targetColumn).encryptedColumn().targetEncColumnName(), e);
                            }
                            if (neededEncryptedColumns.get(targetColumn).encryptedColumn().targetEncMetaColumnName() != null) {
                                String m = encryptedEntity.obtainEncryptedMetaData();
                                row.setJsonb(neededEncryptedColumns.get(targetColumn).encryptedColumn().targetEncMetaColumnName(), m);
                            }
                            break;
                        }
*/
                        row.setText(targetColumn, s.replaceAll("\u0000", ""));
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
/*
                    } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException |
                             InstantiationException | IllegalAccessException e) {
                        LOGGER.error("{}", getStackTrace(e));
                    }
*/
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
                    if (chunk instanceof OraChunk<?>) {
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
//                                    System.out.println(s);
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
//                        System.out.println(timestamp);
                        if (timestamp == null) {
                            row.setTimeStamp(targetColumn, null);
                            break;
                        }
/*
                        long l = timestamp.getTime();
                        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(l),
                                TimeZone.getDefault().toZoneId());
*/
                        LocalDateTime localDateTime = timestamp.toLocalDateTime();
//                        System.out.println(localDateTime);
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
                        if (chunk instanceof OraChunk<?>) {
                            int columnIndex = getColumnIndexByColumnName(fetchResultSet, sourceColumn.toUpperCase());
                            int columnType = fetchResultSet.getMetaData().getColumnType(columnIndex);
                            switch (columnType) {
                                // INTERVALYM
                                case -103:
                                    INTERVALYM intervalym = (INTERVALYM) fetchResultSet.getObject(sourceColumn);
                                    interval = intervalYM2Interval(intervalym);
                                    break;
                                // INTERVALDS
                                case -104:
                                    INTERVALDS intervalds = (INTERVALDS) fetchResultSet.getObject(sourceColumn);
                                    interval = intervalDS2Interval(intervalds);
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
                                log.error("\u001B[31mThere is no handler for type : {}\u001B[0m", targetType);
                                writer.close();
                                connectionTo.close();
                            }
                        } else {
                            log.error("\u001B[31mtryCharIfAny is NULL for type: {}\u001B[0m", targetType);
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
        List<String> strings = new ArrayList<>();
        Map<String, String> columnToColumnMap = config.columnToColumn();
        Map<String, String> expressionToColumnMap = config.expressionToColumn();
        Map<String, EncryptedColumn> encryptedEntityMap = config.expressionToCrypto();
        Map<String, String> cryptoToColumnMap = config.cryptoToColumn();
        if (columnToColumnMap != null) {
            strings.addAll(columnToColumnMap.keySet());
        }
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
                PGKeywords.FROM + " " +
                config.fromSchemaName() +
                "." +
                config.fromTableName() + " " +
                (config.fromTableAlias() == null ? "" : config.fromTableAlias()) + " " +
                (config.fromTableAdds() == null ? "" : config.fromTableAdds()) + " " +
                PGKeywords.WHERE + " " +
                (config.fetchWhereClause() == null ? "" : " ( " + config.fetchWhereClause() + " ) and ") + " " +
                (config.fromTableAlias() == null ? "" : config.fromTableAlias() + ".") +
                "ctid >= " + "concat('(', ? ,',1)')::tid" +
                " and " +
                (config.fromTableAlias() == null ? "" : config.fromTableAlias() + ".") +
                "ctid < " + "concat('(', ? ,',1)')::tid";
    }

    public String buildFetchStatementGreaterXidMin(Config config) {
        return buildFetchStatement(config) + " and " +
                (config.fromTableAlias() == null ? "" : config.fromTableAlias() + ".") +
                "xmin::text::int8 > ? and age(xmin) < age(?::text::xid) and age(xmin) > 0";
    }


    @Override
    public void sync() throws SQLException {
        Connection connection = getConnection();
        Storage targetStorage = StorageService.getStorage(getConnectionProperty().getToProperty(), getConnectionProperty(), false);
        assert targetStorage != null;
        Connection targetConnection = targetStorage.getConnection();
        List<ChunkStatus> chunkStatuses = new ArrayList<>();
        chunkStatuses.add(ChunkStatus.PROCESSED);
        chunkStatuses.add(ChunkStatus.UNCHANGED);
        List<Table> targetTables = createSyncChunksGraterMaxCtidEndPage(connection, chunkStatuses);
        targetTables
                .stream()
                .filter(table -> {
                    try {
                        table.setPkColumns(table.getPrimaryKeyColumns(targetConnection));
                        return table.hasPrimaryKey();
                    } catch (SQLException e) {
                        try {
                            targetConnection.close();
                        } catch (SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                        throw new RuntimeException(e);
                    }
                })
                .findAny()
                .orElseThrow(() -> new RuntimeException("There is no table with PK in target storage!"));
        targetConnection.close();

        Map<Integer, PGChunk<?>> copyChunks = getChunkSyncMap(connection, chunkStatuses);
        createSyncChunks(copyChunks);
        connection.close();

        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        copyChunks.values()
                .forEach(chunk ->
                                service.submit(() -> {
                    try {
                        chunk.setTargetStorage(targetStorage);
                        Connection fromConnection = this.getConnection();
                        Connection toConnection = targetStorage.getConnection();
                        chunk.setSourceConnection(fromConnection);
                        chunk.setTargetConnection(toConnection);
                        chunk.upsertToTarget();
                        chunk.closeChunkSourceConnection(false);
                        chunk.closeChunkTargetConnection();
                    } catch (SQLException e) {
                        log.error(getStackTrace(e));
                        try {
                            chunk.saveChunkStatus(ChunkStatus.PROCESSED_WITH_ERROR, false, null, getStackTrace(e));
                            chunk.closeChunkSourceConnection(true);
                        } catch (SQLException ex) {
                            log.error("{}", getStackTrace(ex));
                            try {
                                chunk.closeChunkSourceConnection(true);
                            } catch (SQLException exc) {
                                log.error("{}",getStackTrace(exc));
                            }
                        }
                        throw new RuntimeException(e);
                    }
                })
                );
        service.shutdown();
        service.close();
/*
        // 3. Check PK at the target tables
//        List<Column> d = chunks.values().stream().findFirst().get().getSourceTable().getPKColumns(connection);
*/
    }

    private List<Table> createSyncChunksGraterMaxCtidEndPage(Connection connection, List<ChunkStatus> statuses) throws SQLException {
        connection.setAutoCommit(false);
        PreparedStatement ps = connection.prepareStatement(SQL_CHUNKS_AVG_SYNC);
        List<Table> tables = new ArrayList<>();
        String[] arr = statuses
                .stream()
                .map(Enum::name)
                .toArray(String[]::new);
        Array array = connection.createArrayOf("VARCHAR", arr);
        ps.setArray(1, array);
        ResultSet rs = ps.executeQuery();
        if (rs.isBeforeFirst()) {
            while (rs.next()) {
                long maxCtidEndPage = rs.getLong("max_ctid_end_page");
                long heapBlksTotal = rs.getLong("heap_blks_total");
                long pagesInChunk = rs.getLong("pages_in_chunk");
                long last_id = rs.getLong("last_id");
                String taskName = rs.getString("task_name");
                String schemaName = rs.getString("schema_name");
                String tableName = rs.getString("table_name");
                String stringConfig = rs.getString("config");
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    Config config = objectMapper.readValue(stringConfig, Config.class);
                    if (heapBlksTotal > maxCtidEndPage) {
                        Table table = new PGTable(schemaName, tableName);
                        insertCtidChunks(
                                connection,
                                config,
                                table,
                                maxCtidEndPage,
                                Math.max((maxCtidEndPage + pagesInChunk), heapBlksTotal),
//                                heapBlksTotal,
                                pagesInChunk,
                                ChunkStatus.UNCHANGED,
                                0,
                                last_id);
                        connection.commit();
                        log.info("NEW Chunk pagesInChunk: {} maxCtidEndPage: {} maxCtidEndPage + pagesInChunk: {} with last_id {}",
                                pagesInChunk, maxCtidEndPage, maxCtidEndPage + pagesInChunk, last_id);

                    }
                    tables.add(new PGTable(config.toSchemaName(), config.toTableName()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        rs.close();
        ps.close();
        ps = connection.prepareStatement(SQL_CHUNKS_SYNC_WITHOUT_XIDMIN);
        ps.setArray(1, array);
        rs = ps.executeQuery();
        if (rs.isBeforeFirst()) {
            while (rs.next()) {
                int chunkId = rs.getInt("chunk_id");
//                int lastId = rs.getInt("last_id");
                PreparedStatement statement = connection.prepareStatement(DML_UPDATE_XID_OF_CTID_CHUNKS_BY_LAST_ID);
                statement.setInt(1, chunkId);
                statement.execute();
                statement.close();
            }
        }
        rs.close();
        ps.close();
        connection.commit();
        return tables;
    }

    private void createSyncChunks(Map<Integer, PGChunk<?>> chunks) {
        try {
            for (PGChunk<?> chunk : chunks.values()) {
                Map.Entry<Long, Long> xidMinMax = chunk.getXidMinMax();
                if (xidMinMax.getKey() > chunk.getXidMin()) {
                    chunk.insertParentChunk(xidMinMax.getKey());
                    log.info("New PARENT ChunkId: {} start: {} end: {} with xidmin {}",
                            chunk.getId(), chunk.getStart(), chunk.getEnd(), xidMinMax.getKey());
                }
            }
        } catch (SQLException | IOException e) {
            log.error("{}", getStackTrace(e));
//            log.error("Error during sync: {}", e.getMessage());
        }
    }

    @Override
    public Map<Integer, PGChunk<?>> getChunkSyncMap(Connection connection, List<ChunkStatus> chunkStatuses) throws SQLException {
        Map<Integer, PGChunk<?>> chunkMap = new HashMap<>();
        PreparedStatement ps = connection.prepareStatement(SQL_CHUNKS_SYNC);
        String[] arr = chunkStatuses
                .stream()
                .map(Enum::name)
                .toArray(String[]::new);
        Array array = connection.createArrayOf("VARCHAR", arr);
        ps.setArray(1, array);
        ResultSet rs = ps.executeQuery();
        if (rs.isBeforeFirst()) {
            while (rs.next()) {
                try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    Config config = objectMapper.readValue(rs.getString("config"), Config.class);
                    chunkMap.put(rs.getInt("chunk_id"), new PGChunk<>(
                            rs.getInt("chunk_id"),
                            rs.getLong("start_page"),
                            rs.getLong("end_page"),
                            config,
                            new PGTable(config.fromSchemaName(), config.fromTableName()),
                            new PGTable(config.toSchemaName(), config.toTableName()),
                            this,
                            rs.getInt("parent_id"),
                            rs.getLong("xidmin"),
                            rs.getLong("xidmax"),
                            connection,
                            buildFetchStatementGreaterXidMin(config)
                    ));
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }
        }
        rs.close();
        ps.close();
        return chunkMap;
    }

    @Override
    public void createPrimaryKey(Map<Table, Table> tables, Storage targetStorage) {
        try {
            Connection sourceConnection = getConnection();
            Connection targetConnection = targetStorage.getConnection();
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table sourceTable = entry.getKey();
                Table targetTable = entry.getValue();
                List<Column> sourcePKColumns = sourceTable.getPkColumns();
                if (!sourcePKColumns.isEmpty()) {
                    sourceTable.setPkColumns(sourcePKColumns);
                    targetTable.setPkColumns(sourcePKColumns);
                    targetTable.createPrimaryKey(targetConnection);
//                    log.info("Creating primary key for table {}.{} in target storage", targetTable.getSchemaName(), targetTable.getTableName());
                } else {
                    throw new SQLException("Source table " + sourceTable.getSchemaName() + "." + sourceTable.getTableName() +
                            " has no primary key, cannot create primary key in target table " +
                            targetTable.getSchemaName() + "." + targetTable.getTableName());
                }
            }
            sourceConnection.close();
            targetConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void createIndex(Map<Table, Table> tables, Storage targetStorage) {
        try {
            Connection sourceConnection = getConnection();
            Connection targetConnection = targetStorage.getConnection();
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table sourceTable = entry.getKey();
                sourceTable.createIndex(sourceConnection);
            }
            sourceConnection.close();
            targetConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public Map<Table, Table> getMapOfTables(List<Config> configs, Storage targetStorage) {
        Map<Table, Table> tables = new HashMap<>();
        for (Config c : configs) {
            tables.put(new  PGTable(c.fromSchemaName(), c.fromTableName()), targetStorage.createTable(c));
        }
        return tables;
    }

    @Override
    public Map<Table, Table> enrichMapOfTables(Map<Table, Table> tables, Storage targetStorage) {
        Map<Table, Table> enrichedTables = new HashMap<>();
        try {
            Connection sourceConnection = getConnection();
            Connection targetConnection = targetStorage.getConnection();
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table sourceTable = entry.getKey();
                Table targetTable = entry.getValue();
                List<Column> allSourceColumns = sourceTable.getAllColumns(sourceConnection);
                List<Column> sourcePKColumns = sourceTable.getPrimaryKeyColumns(sourceConnection);
                List<Index> sourceIndexes = sourceTable.getIndexes(sourceConnection);
                sourceTable.setPkColumns(allSourceColumns);
                sourceTable.setPkColumns(sourcePKColumns);
                sourceTable.setIndexes(sourceIndexes);
                targetTable.setPkColumns(sourcePKColumns);
                enrichedTables.put(sourceTable, targetTable);
            }
            sourceConnection.close();
            targetConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
        return enrichedTables;
    }

    @Override
    public Table createTable(Config config) {
        return new PGTable(config.toSchemaName(), config.toTableName());
    }
}
