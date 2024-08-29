package org.bublik.storage;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.pgsql.model.interval.Interval;
import de.bytefish.pgbulkinsert.pgsql.model.range.Range;
import de.bytefish.pgbulkinsert.row.SimpleRow;
import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import org.bublik.exception.TableNotExistsException;
import org.bublik.model.*;
import org.bublik.service.JDBCStorageService;
import org.bublik.service.StorageService;
import org.bublik.service.TableService;
import org.postgresql.PGConnection;
import org.postgresql.util.PGInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.function.Consumer;

import static org.bublik.constants.SQLConstants.*;
import static org.bublik.exception.Utils.getStackTrace;
import static org.bublik.util.ColumnUtil.*;

public class JDBCPostgreSQLStorage extends JDBCStorage implements JDBCStorageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCPostgreSQLStorage.class);
    private static JDBCPostgreSQLStorage instance;

    private JDBCPostgreSQLStorage(StorageClass storageClass, ConnectionProperty connectionProperty) throws SQLException {
        super(storageClass, connectionProperty);
    }

    public static synchronized JDBCPostgreSQLStorage getInstance(StorageClass storageClass,
                                                             ConnectionProperty connectionProperty) throws SQLException{
        if (instance == null) {
            instance = new JDBCPostgreSQLStorage(storageClass, connectionProperty);
        }
        return instance;
    }

/*
    @Override
    public boolean hook(List<Config> configs) throws SQLException {
        if (getConnectionProperty().getInitPGChunks()) {
            fillCtidChunks(configs);
            if (getConnectionProperty().getRowsStat()) {
                fillRowsStat(configs);
            }
        }
        return getConnectionProperty().getCopyPGChunks() == null || getConnectionProperty().getCopyPGChunks();
    }
*/

    @Override
    public Map<Integer, Chunk<?>> getChunkMap(List<Config> configs) throws SQLException {
        Map<Integer, Chunk<?>> chunkHashMap = new TreeMap<>();
        String sql = buildStartEndOfChunk(configs);
        PreparedStatement statement = initialConnection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.isBeforeFirst()) {
//            Storage targetStorage = StorageService.getStorage(getConnectionProperty().getToProperty(), getConnectionProperty());
//            StorageService.set(targetStorage);
            while (resultSet.next()) {
                Config config = findByTaskName(configs, resultSet.getString("task_name"));
                Table sourceTable = TableService.getTable(initialConnection, config.fromSchemaName(), config.fromTableName());
                if (!sourceTable.exists(initialConnection)) {
                    initialConnection.close();
                    LOGGER.error("\u001B[31mThe Source Table: {}.{} does not exist.\u001B[0m", sourceTable.getSchemaName(),
                            sourceTable.getTableName());
                    throw new TableNotExistsException(sourceTable.getSchemaName(), sourceTable.getTableName());
                }
                chunkHashMap.put(resultSet.getInt("rownum"),
                        new PGChunk<>(
                                resultSet.getInt("chunk_id"),
                                resultSet.getLong("start_page"),
                                resultSet.getLong("end_page"),
                                config,
                                sourceTable,
                                this//,
//                                targetStorage
                        )
                );
            }
        }
        resultSet.close();
        statement.close();
        return chunkHashMap;
    }

    private void fillCtidChunks(List<Config> configs) {
        createTableCtidChunks();
        try {
            for (Config config : configs) {
                long reltuples = 0;
                long relpages = 0;
                long max_end_page = 0;
                long heap_blks_total = 0;
                PreparedStatement preparedStatement = initialConnection.prepareStatement(SQL_NUMBER_OF_TUPLES);
                Table table = TableService.getTable(initialConnection, config.fromSchemaName(), config.fromTableName());
                preparedStatement.setString(1, table.getSchemaName().toLowerCase());
                preparedStatement.setString(2, table.getFinalTableName(false));
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    reltuples = resultSet.getLong("reltuples");
                    relpages = resultSet.getLong("relpages");
                }
                resultSet.close();
                preparedStatement.close();
                preparedStatement = initialConnection.prepareStatement(SQL_NUMBER_OF_RAW_TUPLES);
                preparedStatement.setString(1, table.getSchemaName().toLowerCase() + "." +
                        table.getTableName());
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    heap_blks_total = resultSet.getLong("heap_blks_total");
                }
                resultSet.close();
                preparedStatement.close();
                double rowsInChunk = reltuples >= 500000 ? ROWS_IN_CHUNK : 10000d;
//            double rowsInChunk = 10000d;
                long v = reltuples <= 0 && relpages <= 1 ? relpages + 1 :
                        (int) Math.round(relpages / (reltuples / rowsInChunk));
                long pagesInChunk = Math.min(v, relpages + 1);
                LOGGER.info("{}.{} \t\t\t relpages : {}\t heap_blks_total : {}\t reltuples : {}\t rowsInChunk : {}\t pagesInChunk : {} ",
                        config.fromSchemaName(),
                        config.fromTableName(),
                        relpages,
                        heap_blks_total,
                        reltuples,
                        rowsInChunk,
                        pagesInChunk);
                PreparedStatement chunkInsert = initialConnection.prepareStatement(DML_BATCH_INSERT_CTID_CHUNKS);
                chunkInsert.setLong(1, pagesInChunk);
                chunkInsert.setLong(2, 0);
                chunkInsert.setString(3, config.fromTaskName());
                chunkInsert.setString(4, table.getSchemaName().toLowerCase());
                chunkInsert.setString(5, table.getFinalTableName(false));
                chunkInsert.setLong(6, relpages);
                chunkInsert.setLong(7, pagesInChunk);
//            System.out.println(chunkInsert);
                int rows = chunkInsert.executeUpdate();
                chunkInsert.close();

                preparedStatement = initialConnection.prepareStatement(SQL_MAX_END_PAGE);
                preparedStatement.setString(1, config.fromTaskName());
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    max_end_page = resultSet.getLong("max_end_page");
                }
                resultSet.close();
                preparedStatement.close();
                if (heap_blks_total > max_end_page) {
                    chunkInsert = initialConnection.prepareStatement(DML_INSERT_CTID_CHUNKS);
                    chunkInsert.setLong(1, max_end_page);
                    chunkInsert.setLong(2, heap_blks_total);
                    chunkInsert.setLong(3, 0);
                    chunkInsert.setString(4, config.fromTaskName());
                    chunkInsert.setString(5, table.getSchemaName().toLowerCase());
                    chunkInsert.setString(6, table.getFinalTableName(false));
                    rows = chunkInsert.executeUpdate();
                    chunkInsert.close();
                }
            }
            initialConnection.commit();
        } catch (SQLException e) {
            LOGGER.error("{}", getStackTrace(e));
        }
    }

    private void createTableCtidChunks() {
        try {
            Statement createTable = initialConnection.createStatement();
            createTable.executeUpdate(DDL_CREATE_POSTGRESQL_TABLE_CHUNKS);
            createTable.close();
        } catch (SQLException e) {
            LOGGER.error("{}", getStackTrace(e));
        }
    }

    private void fillRowsStat(List<Config> configs) throws SQLException {
        Statement statement = initialConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(SQL_CHUNKS);
        while(resultSet.next()) {
            int chunk_id = resultSet.getInt("chunk_id");
            long start_page = resultSet.getLong("start_page");
            long end_page = resultSet.getLong("end_page");
            String schema_name = resultSet.getString("schema_name");
            String table_name = resultSet.getString("table_name");
            PreparedStatement rowCountSQL = initialConnection.prepareStatement(
                    SQL_NUMBER_OF_TUPLES_PER_CHUNK_P1 +
                    schema_name + "." +
                    table_name +
                    SQL_NUMBER_OF_TUPLES_PER_CHUNK_P2);
            rowCountSQL.setLong(1, start_page);
            rowCountSQL.setLong(2, end_page);
            ResultSet set = rowCountSQL.executeQuery();
            while(set.next()) {
                long rows = set.getLong("rows");
                PreparedStatement updateRowsOfCtid = initialConnection.prepareStatement(DML_UPDATE_CTID_CHUNKS);
                updateRowsOfCtid.setLong(1, rows);
                updateRowsOfCtid.setInt(2, chunk_id);
                updateRowsOfCtid.execute();
                updateRowsOfCtid.close();
            }
            set.close();
            rowCountSQL.close();
            initialConnection.commit();
        }
        resultSet.close();
        statement.close();
    }

    @Override
    public String buildStartEndOfChunk(List<Config> configs) {
        List<String> taskNames = new ArrayList<>();
        configs.forEach(sqlStatement -> taskNames.add(sqlStatement.fromTaskName()));
        return "select row_number() over (order by chunk_id) as rownum, chunk_id, start_page, end_page, task_name from public.ctid_chunks where task_name in ('" +
                String.join("', '", taskNames) + "') " +
                "and status <> 'PROCESSED' ";
    }

    @Override
    public LogMessage transferToTarget(Chunk<?> chunk) throws SQLException {
        ResultSet fetchResultSet = chunk.getResultSet();
        if (fetchResultSet.next()) {
            Connection connectionTo = getConnection();
            Table table = TableService.getTable(connectionTo, chunk.getConfig().toSchemaName(), chunk.getConfig().toTableName());
//            System.out.println(table.exists(connectionTo));
            if (table.exists(connectionTo)) {
                chunk.setTargetTable(table);
                try {
                    return fetchAndCopy(connectionTo, fetchResultSet, chunk);
                } catch (RuntimeException e) {
                    connectionTo.close();
                    throw e;
                } finally {
                    connectionTo.close();
                }
            } else {
                LOGGER.error("\u001B[31mThe Target Table: {}.{} does not exist.\u001B[0m", chunk.getConfig().toSchemaName(),
                        chunk.getConfig().toTableName());
                throw new TableNotExistsException("The Target Table "
                        + chunk.getConfig().toSchemaName() + "."
                        + chunk.getConfig().toTableName() + " does not exist.");
            }
        } else {
            return new LogMessage(
                    0,
                    0,
                    0,
                    "NO ROWS FETCH",
                    chunk);
        }
    }

    private LogMessage fetchAndCopy(Connection connectionTo,
                                    ResultSet fetchResultSet,
                                    Chunk<?> chunk) {
        int recordCount = 0;
        Map<String, PGColumn> neededColumnsToDB = readTargetColumnsAndTypes(connectionTo, chunk);
        Map<List<String>, PGColumn> neededColumnsFromMany = readTargetColumnsAndTypesFromMany(connectionTo, chunk);
/*
        if (neededColumnsFromMany != null) {
            neededColumnsFromMany.forEach((k,v) ->
                    System.out.println(k + v.getColumnName()));
        }
*/
        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connectionTo);
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
        try {
            SimpleRowWriter writer = new SimpleRowWriter(table, pgConnection);
            Consumer<SimpleRow> simpleRowConsumer =
                    s -> {
                            try {
                                simpleRowConsume(s, neededColumnsToDB, neededColumnsFromMany, fetchResultSet, chunk, connectionTo, writer);
                            } catch (RuntimeException e) {
                                LOGGER.error("{}.{} {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                            }
                    };
            do {
                writer.startRow(simpleRowConsumer);
                recordCount++;
            } while (fetchResultSet.next());
            writer.close();
            connectionTo.commit();
        } catch (Exception e) {
            LOGGER.error("{}.{} {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
            throw new RuntimeException(e);
        }
        long stop = System.currentTimeMillis();
        return new LogMessage(
                recordCount,
                start,
                stop,
                "PostgreSQL COPY",
                chunk);
    }

    public Map<String, PGColumn> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?> chunk) {
        Map<String, PGColumn> columnMap = new HashMap<>();
        try {
            ResultSet resultSet;
            resultSet = connectionTo.getMetaData().getColumns(
                    null,
                    chunk.getTargetTable().getSchemaName().toLowerCase(),
                    chunk.getTargetTable().getFinalTableName(false),
                    null);
            Map<String, String> columnToColumnMap = chunk.getConfig().columnToColumn();
            Map<String, String> expressionToColumnMap = chunk.getConfig().expressionToColumn();
            Map<String, List<String>> columnFromManyMap = chunk.getConfig().columnFromMany();

            while (resultSet.next()) {
                String columnName = resultSet.getString(4);
                String columnType = resultSet.getString(6);
                Integer columnPosition = resultSet.getInt(17);

                if (columnToColumnMap != null) {
                    columnToColumnMap
                            .entrySet()
                            .stream()
                            .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(columnName))
                            .forEach(i -> columnMap.put(i.getKey(), new PGColumn(columnPosition, i.getValue(), columnType.equals("bigserial") ? "bigint" : columnType)));
                }

                if (expressionToColumnMap != null) {
                    expressionToColumnMap
                            .entrySet()
                            .stream()
                            .filter(s -> s.getValue().replaceAll("\"", "").equalsIgnoreCase(columnName))
                            .forEach(i -> columnMap.put(columnName, new PGColumn(columnPosition, i.getValue(), columnType.equals("bigserial") ? "bigint" : columnType)));
                }

                if (columnFromManyMap != null) {
                    columnFromManyMap
                            .entrySet()
                            .stream()
                            .filter(s -> s.getKey().replaceAll("\"", "").equalsIgnoreCase(columnName))
                            .forEach(i -> columnMap.put(i.getKey(), new PGColumn(columnPosition, i.getKey(), columnType.equals("bigserial") ? "bigint" : columnType)));
                }
            }
            resultSet.close();
        } catch (SQLException e) {
            LOGGER.error("{}", e.getMessage());
        }
        return columnMap;
    }

    public Map<List<String>, PGColumn> readTargetColumnsAndTypesFromMany(Connection connectionTo, Chunk<?> chunk) {
        Map<List<String>, PGColumn> columnMap = new HashMap<>();
        try {
            ResultSet resultSet;
            resultSet = connectionTo.getMetaData().getColumns(
                    null,
                    chunk.getTargetTable().getSchemaName().toLowerCase(),
                    chunk.getTargetTable().getFinalTableName(false),
                    null);
            Map<String, List<String>> columnFromManyMap = chunk.getConfig().columnFromMany();

            while (resultSet.next()) {
                String columnName = resultSet.getString(4);
                String columnType = resultSet.getString(6);
                Integer columnPosition = resultSet.getInt(17);

                if (columnFromManyMap != null) {
                    columnFromManyMap
                            .entrySet()
                            .stream()
                            .filter(s -> s.getKey().replaceAll("\"", "").equalsIgnoreCase(columnName))
                            .forEach(i -> columnMap.put(i.getValue(), new PGColumn(columnPosition, i.getKey(), columnType.equals("bigserial") ? "bigint" : columnType)));
                }
            }
            resultSet.close();
        } catch (SQLException e) {
            LOGGER.error("{}", getStackTrace(e));
        }
        return columnMap;
    }

    private void simpleRowConsume(SimpleRow row,
                                  Map<String, PGColumn> neededColumnsToDB,
                                  Map<List<String>, PGColumn> neededColumnsFromMany,
                                  ResultSet fetchResultSet,
                                  Chunk<?> chunk,
                                  Connection connectionTo,
                                  SimpleRowWriter writer) {
//        System.out.println("simpleRowConsume...");
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
                    try {
                        String s = fetchResultSet.getString(sourceColumn);
                        if (s == null) {
                            row.setText(targetColumn, null);
                            break;
                        }
                        row.setText(targetColumn, s.replaceAll("\u0000", ""));
                        break;
                    } catch (SQLException e) {
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                    }
                }
                case "bpchar":
                    try {
                        String string = fetchResultSet.getString(sourceColumn);
                        row.setText(targetColumn, string);
                        break;
                    } catch (SQLException e) {
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
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
                    } catch (SQLException e) {
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
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
                    } catch (SQLException e) {
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
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
                    } catch (SQLException e) {
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
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
                    } catch (SQLException e) {
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
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
                    } catch (SQLException e) {
                        LOGGER.error("\u001B[31m{}.{} {} -> {}\u001B[0m: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
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
                    } catch (SQLException e) {
                        LOGGER.error("\u001B[31m{}.{} {} -> {}\u001B[0m: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
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
                    } catch (SQLException e) {
                        LOGGER.error("\u001B[31m{}.{} {} -> {}\u001B[0m: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
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
                    } catch (SQLException e) {
                        LOGGER.error("\u001B[31m{}.{} {} -> {}\u001B[0m: {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
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
                    } catch (SQLException e) {
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                    }
                }
                case "timestamp": {
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
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                    }
                }
                case "timestamptz": {
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
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                    }
                }
                case "date":
                    try {
                        Date date = fetchResultSet.getDate(sourceColumn);
                        if (date == null) {
                            row.setDate(targetColumn, null);
                            break;
                        }
/*
                        Timestamp timestamp = fetchResultSet.getTimestamp(sourceColumn);
                        if (timestamp == null) {
                            row.setDate(targetColumn, null);
                            break;
                        }
                        long l = timestamp.getTime();
                        LocalDate localDate = Instant.ofEpochMilli(l)
                                .atZone(ZoneId.systemDefault()).toLocalDate();
                        System.out.println(timestamp + " " + l + " : " + localDate);
                        row.setDate(targetColumn, localDate);
*/
                        row.setDate(targetColumn, date.toLocalDate());
                        break;
                    } catch (SQLException e) {
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
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
                    } catch (SQLException e) {
                        LOGGER.error("\u001B[31mtstzrange\u001B[0m : {}.{} - {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                        throw new RuntimeException();
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
                    } catch (SQLException e) {
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
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
                    } catch (SQLException e) {
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
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
                    } catch (SQLException e) {
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
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
                                e1.printStackTrace();
                                System.out.println(targetColumn + " : " + o);
                            }
                        }
                        row.setUUID(targetColumn, uuid);
                        break;
                    } catch (SQLException e) {
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
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
                                LOGGER.error("\u001B[31mThere is no handler for type : {}\u001B[0m", targetType);
                                writer.close();
                                connectionTo.close();
                            }
                        } else {
                            LOGGER.error("\u001B[31mtryCharIfAny is NULL\u001B[0m");
                            throw new RuntimeException();
                        }
                    } catch (SQLException e) {
                        LOGGER.error("{}.{} : {}", chunk.getTargetTable().getSchemaName(), chunk.getTargetTable().getTableName(), getStackTrace(e));
                    }
            }
        }
    }
}
