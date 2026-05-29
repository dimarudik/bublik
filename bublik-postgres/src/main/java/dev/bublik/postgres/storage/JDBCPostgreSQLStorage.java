package dev.bublik.postgres.storage;

import de.bytefish.pgbulkinsert.exceptions.BinaryWriteFailedException;
import de.bytefish.pgbulkinsert.pgsql.model.interval.Interval;
import de.bytefish.pgbulkinsert.row.SimpleRow;
import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.constants.PGKeywords;
import dev.bublik.core.exception.SourceSQLException;
import dev.bublik.core.exception.TargetSQLException;
import dev.bublik.core.model.*;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import dev.bublik.postgres.model.PGChunk;
import dev.bublik.postgres.model.PGTable;
import dev.bublik.postgres.model.PgIntervalComponents;
import dev.bublik.postgres.service.StreamApiService;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static dev.bublik.core.constants.CLassConstants.ORACLE_STORAGE_CLASS_NAME;
import static dev.bublik.core.util.ColumnUtil.*;
import static dev.bublik.core.util.Utils.getStackTrace;
import static dev.bublik.postgres.constants.SQLConstants.*;
import static dev.bublik.postgres.util.ColumnUtil.*;

public class JDBCPostgreSQLStorage<K extends Integer, T extends Long, S extends Connection, R extends ResultSet> extends JDBCStorage<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(JDBCPostgreSQLStorage.class);
    StreamApiService streamApiService = new StreamApiService();


    public JDBCPostgreSQLStorage(ConnectionProperty connectionProperty) throws SQLException {
        super(connectionProperty);
    }

    public JDBCPostgreSQLStorage(StorageClass storageClass, ConnectionProperty connectionProperty) throws SQLException {
        super(storageClass, connectionProperty);
    }

    @Override
    public List<Chunk<K, T, S, R>> getChunkList(List<Config> configs, String chunkTableName, Storage<K, T, S, R> targetStorage) throws SQLException {
        List<Chunk<K, T, S, R>> chunks = new ArrayList<>();
        for (Config config : configs) {
            Table<S> sourceTable = this.configToTable(config.fromSchemaName(), config.fromTableName());
            Table<S> targetTable = targetStorage.configToTable(config.toSchemaName(), config.toTableName());
            this.enrichTable(sourceTable);
            targetStorage.enrichTable(sourceTable, targetTable);
            List<Column2Column> c2c = getColumn2Column(sourceTable, targetTable, config);
            Table2Table<S> t2t = getTable2Table(sourceTable, targetTable, c2c, config);
            String sql = buildStartEndOfChunk(config, chunkTableName, sourceTable);
            log.debug("Query of chunks for table {}.{}: {}", t2t.sourceTable().getSchemaName(), t2t.sourceTable().getTableName(), sql);
            String fetchQuery = buildFetchStatement(config, t2t);
            String orderByClause = targetTable.buildOrderBy(config);
            log.info("Fetch query: {} {}", fetchQuery, orderByClause);
            S sourceSession = this.getPoolConnection();
            PreparedStatement preparedStatement = sourceSession.prepareStatement(sql);
            preparedStatement.setString(1, config.fromSchemaName());
            preparedStatement.setString(2, config.fromTableName());
            preparedStatement.setString(3, config.fromTaskName());
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                String status = rs.getString("status");
                Chunk<K, T, S, R> chunk = new PGChunk<>(
                        (K) (Integer) rs.getInt("chunk_id"),
                        (T) (Long) rs.getLong("start_page"),
                        (T) (Long) rs.getLong("end_page"),
                        config,
                        t2t,
                        ChunkStatus.valueOf(status),
                        fetchQuery,
                        this,
                        targetStorage,
                        orderByClause);
                chunks.add(chunk);
            }
            rs.close();
            preparedStatement.close();
            sourceSession.close();
        }
        return chunks;
    }

/*
    private String getOrderByIfExists(Table<S> targetTable, Config config, String alias) {
        if (config.columnToColumn() != null && config.expressionToColumn() != null) {
            return targetTable.buildOrderBy(targetTable.getPkColumns(), alias);
        }
        return "";
    }
*/

    @Override
    public Table2Table<S> getTable2Table(Table<S> sourceTable,
                                          Table<S> targetTable,
                                          List<Column2Column> c2c,
                                          Config config) {
        Column ttlColumn = null;
        Column timestampColumn = null;
        if (config.withTTL() != null) {
            ttlColumn = new Column(-1,
                    "_ttl",
                    "int",
                    null,
                    null,
                    config.withTTL(),
                    null,
                    null,
                    0,
                    null,
                    0,
                    null,
                    false,
                    false,
                    false);
        }
        if (config.timestamp() != null) {
            timestampColumn = new Column(-1,
                    "_timestamp",
                    "int",
                    null,
                    null,
                    config.timestamp(),
                    null,
                    null,
                    0,
                    null,
                    0,
                    null,
                    false,
                    false,
                    false);
        }
        return new Table2Table<>(sourceTable, targetTable, c2c, ttlColumn, timestampColumn);
    }

/*
    private void logColumn2Column(List<Column2Column> column2Column) {
        column2Column.forEach(c2c -> log.info("Column2Column: {} {} {} -> {} {}",
                c2c.sourceExpression(),
                c2c.sourceColumn().columnName(), c2c.sourceColumn().columnType(),
                c2c.targetColumn().columnName(), c2c.targetColumn().columnType()));
    }
*/

    @Override
    public List<Column2Column> getColumn2Column(Table<S> sourceTable, Table<S> targetTable, Config config) {
        List<Column2Column> column2Column = new ArrayList<>();
        if (config.columnToColumn() == null && config.expressionToColumn() == null && config.asList() == null) {
            if (sourceTable.getClass() == targetTable.getClass()) {
                sourceTable.getColumns().forEach(c -> column2Column.add(new Column2Column(c, c)));
            } else {
                column2Column.addAll(matchColumns(sourceTable, targetTable));
/*
                sourceTable.getColumns().forEach(c -> column2Column.add(new Column2Column(c,
                        targetTable
                                .getColumns()
                                .stream()
                                .filter(c1 -> c1.columnName().equals(c.columnName())).findFirst().orElseThrow())));
*/
            }
        }
        if (config.columnToColumn() != null) {
            for (Map.Entry<String,String> entry : config.columnToColumn().entrySet()) {
                Column sourceColumn = sourceTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes()
                                .equalsIgnoreCase(entry.getKey().replaceAll("\"", "")))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(entry.getKey() + " not found in source table " +
                                sourceTable.getSchemaName() + "." + sourceTable.getTableName()));
                Column targetColumn = targetTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes()
                                .equalsIgnoreCase(entry.getValue().replace("\"", "")))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(entry.getValue() + " not found in target table " +
                                targetTable.getSchemaName() + "." + targetTable.getTableName()));
                column2Column.add(new Column2Column(sourceColumn, targetColumn));
            }
        }
        if (config.expressionToColumn() != null) {
            for (Map.Entry<String,String> entry : config.expressionToColumn().entrySet()) {
                Column column = targetTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes()
                                .equalsIgnoreCase(entry.getValue().replace("\"", "")))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(entry.getValue() + " not found in target table " +
                                targetTable.getSchemaName() + "." + targetTable.getTableName()));
                column2Column.add(new Column2Column(column, column, entry.getKey()));
            }
        }
        if (config.asList() != null) {
            for (Map.Entry<String,List<String>> entry : config.asList().entrySet()) {
                String targetColumnName = entry.getKey();
                List<String> sourceColumns = new ArrayList<>();
                int i = 0;
                for (String column : entry.getValue()) {
                    if (isColumnNameWithAsConstruction(column)) {
                        sourceColumns.add(column);
                    } else {
                        sourceColumns.add(column + " as " + targetColumnName + i++);
                    }
                }
                Column targetColumn = targetTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes()
                                .equalsIgnoreCase(entry.getKey().replace("\"", "")))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(entry.getKey() + " not found in target table " +
                                targetTable.getSchemaName() + "." + targetTable.getTableName()));
                column2Column.add(new Column2Column(null, targetColumn, null, sourceColumns, null, null, null));
            }
        }
        if (config.asSet() != null) {
            for (Map.Entry<String,List<String>> entry : config.asSet().entrySet()) {
                String targetColumnName = entry.getKey();
                List<String> sourceColumns = new ArrayList<>();
                int i = 0;
                for (String column : entry.getValue()) {
                    if (isColumnNameWithAsConstruction(column)) {
                        sourceColumns.add(column);
                    } else {
                        sourceColumns.add(column + " as " + targetColumnName + i++);
                    }
                }
                Column targetColumn = targetTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes()
                                .equalsIgnoreCase(entry.getKey().replace("\"", "")))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(entry.getKey() + " not found in target table " +
                                targetTable.getSchemaName() + "." + targetTable.getTableName()));
                column2Column.add(new Column2Column(null, targetColumn, null, null, sourceColumns, null, null));
            }
        }
        if (config.asMap() != null) {
            for (Map.Entry<String, List<KV>> entry : config.asMap().entrySet()) {
                String targetColumnName = entry.getKey();
                List<KV> sourceColumns = new ArrayList<>();
                int k = 0;
                int v = 0;
                for (KV kv : entry.getValue()) {
                    String key;
                    if (isColumnNameWithAsConstruction(kv.key())) {
                        key = kv.key();
                    } else {
                        key = kv.key() + " as _K_" + targetColumnName + k++;
                    }
                    String value;
                    if (isColumnNameWithAsConstruction(kv.value())) {
                        value = kv.value();
                    } else {
                        value = kv.value() + " as _V_" + targetColumnName + v++;
                    }
                    sourceColumns.add(new KV(key, value));
                }
                Column targetColumn = targetTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes()
                                .equalsIgnoreCase(entry.getKey().replace("\"", "")))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(entry.getKey() + " not found in target table " +
                                targetTable.getSchemaName() + "." + targetTable.getTableName()));
                column2Column.add(new Column2Column(null, targetColumn, null, null, null, sourceColumns, null));
            }
        }
        if (config.asUDT() != null) {
            for (Map.Entry<String,List<String>> entry : config.asUDT().entrySet()) {
                String targetColumnName = entry.getKey();
                List<String> sourceColumns = new ArrayList<>();
                int i = 0;
                for (String column : entry.getValue()) {
                    if (isColumnNameWithAsConstruction(column)) {
                        sourceColumns.add(column);
                    } else {
                        sourceColumns.add(column + " as " + targetColumnName + i++);
                    }
                }
                Column targetColumn = targetTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes()
                                .equalsIgnoreCase(entry.getKey().replace("\"", "")))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(entry.getKey() + " not found in target table " +
                                targetTable.getSchemaName() + "." + targetTable.getTableName()));
                column2Column.add(new Column2Column(null, targetColumn, null, null, null, null, sourceColumns));
            }
        }
//        logColumn2Column(column2Column);
        return column2Column;
    }

    @Override
    public String buildStartEndOfChunk(Config config, String chunkTableName, Table<S> sourceTable) {
        return "select chunk_id, uuid, start_page, end_page, task_name, status from " +
                chunkTableName + " where " +
                "schema_name = ? and table_name = ? and task_name = ? " +
                " and status in ('ASSIGNED', 'UNASSIGNED', 'PROCESSED_WITH_ERROR') "
                + " limit 1000 ";
    }

    @Override
    public LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException,
//            BinaryWriteFailedException,
            SourceSQLException, TargetSQLException {
        if (chunk.getSourceStorage() instanceof JDBCStorage<K,T,S,R>) {
            ResultSet fetchResultSet = chunk.getResultSet();
            Connection connectionFrom = chunk.getSourceSession();
            if (fetchResultSet.next()) {
                Connection connectionTo = chunk.getTargetSession();
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
                }  catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    ;
                }
            } else {
                return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), "NO ROWS FETCH");
            }
        } else {
            return chunk.getSourceStorage().transfer(chunk, tableName);
        }
    }

    private boolean hasNext(ResultSet resultSet) {
        try {
            return resultSet.next();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Deprecated
    @Override
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?, ?, ?> chunk) {
        Map<String, Column> columnMap = new HashMap<>();
        try {
            ResultSet resultSet = connectionTo.getMetaData().getColumns(
                    null,
                    chunk.getT2t().targetTable().getSchemaName().toLowerCase(),
                    chunk.getT2t().targetTable().getTableNameWithoutQuotes(),
//                    chunk.getT2t().targetTable().getFinalTableName(false),
                    null);
            Map<String, String> columnToColumnMap = chunk.getConfig().columnToColumn();
            Map<String, String> expressionToColumnMap = chunk.getConfig().expressionToColumn();
            Map<String, List<String>> columnFromManyMap = chunk.getConfig().columnFromMany();

            log.info("Target table: {}.{}", chunk.getT2t().targetTable().getSchemaName().toLowerCase(), chunk.getT2t().targetTable().getTableNameWithoutQuotes());

            while (resultSet.next()) {
                String columnName = resultSet.getString(4);
                Integer dataType = resultSet.getInt(5);
                String columnType = resultSet.getString(6);
                Integer columnPosition = resultSet.getInt(17);

                log.info("columnName: {}, dataType: {}, columnType: {}, columnPosition: {}",
                       columnName, dataType, columnType, columnPosition);

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
                                            dataType, null, null, null, null, 0, null, 0, null, false, false, false)));
                } else if (expressionToColumnMap == null) {
                    Table<?> sourceTable = chunk.getT2t().sourceTable();
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
                                            dataType, null, null, null, null, 0 , null, 0, null, false, false, false)));
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
                                            dataType, null, null, null, null, 0 , null, 0, null, false, false, false)));
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
                    chunk.getT2t().targetTable().getSchemaName().toLowerCase(),
                    chunk.getT2t().targetTable().getFinalTableName(false),
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
                                            dataType, null, null, null, null, 0 , null, 0, null, false, false, false)));
                }
            }
            resultSet.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
        return columnMap;
    }

/*
    private LogMessage fetchAndCopy(ResultSet fetchResultSet,
                                    Chunk<?, ?, ?, ?> chunk,
                                    String tableName) throws SQLException, BinaryWriteFailedException, SourceSQLException{
        int recordCount = 0;
        Connection connectionTo = (Connection) chunk.getTargetSession();

        if (!isChunkProcessed(chunk, tableName)) {
            Map<String, Column> columnToColumnMap = chunk.getT2t().column2Columns()
                    .stream()
                    .collect(Collectors.toMap(el -> el.sourceColumn().columnName(), Column2Column::targetColumn));

            Map<List<String>, Column> neededColumnsFromMany = readTargetColumnsAndTypesFromMany(connectionTo, chunk);

            SimpleRowWriter writer = streamApiService.getSimpleRowWriter(connectionTo, chunk,
                    chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getFinalTableName(true));

            do {
                writer.startRow(s -> {
                    try {
                        simpleRowConsume(s, columnToColumnMap, neededColumnsFromMany,
                                fetchResultSet, chunk, connectionTo, writer);
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
                    }
                });
                recordCount++;
            } while (hasNext(fetchResultSet));

            streamApiService.closeSimpleRowWriter(writer);

            chunk.setCopied(recordCount);
            insertProcessedChunkInfo(chunk, tableName);
            connectionTo.commit();

            return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), "PostgreSQL COPY");
        } else {
            return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), "The chunk has already been copied");
        }
    }
*/

    private LogMessage fetchAndCopy(ResultSet fetchResultSet,
                                    Chunk<?, ?, ?, ?> chunk,
                                    String tableName) throws SQLException, SourceSQLException, IOException {
        int recordCount = 0;
        Connection connectionTo = (Connection) chunk.getTargetSession();

        if (!isChunkProcessed(chunk, tableName)) {
            List<Column2Column> columnToColumnMap = chunk.getT2t().column2Columns();
            List<String> columnNames = columnToColumnMap.stream().map(Column2Column::targetColumn).map(Column::columnName).toList();

            String tableNameWithSchema = chunk.getT2t().targetTable().getSchemaName() + "." +
                    chunk.getT2t().targetTable().getFinalTableName(true);
            String sql = "COPY " + tableNameWithSchema + " (" + String.join(", ", columnNames) + ") FROM STDIN BINARY";
//            System.out.println(sql);

            int pgStreamBufferSize = 1024 * 1024;
            int javaBufferSize = 64 * 1024;
            PGConnection pgConnection = connectionTo.unwrap(PGConnection.class);
            try (PGCopyOutputStream os = new PGCopyOutputStream(pgConnection, sql, pgStreamBufferSize);
                 PgBinaryWriter writer = new PgBinaryWriter(os, javaBufferSize)) {
                do {
                    writer.startRow((short) columnNames.size());
                    writeValue(columnToColumnMap, fetchResultSet, chunk, writer);
                    recordCount++;
                } while (hasNext(fetchResultSet));
            }

            chunk.setCopied(recordCount);
            insertProcessedChunkInfo(chunk, tableName);
            connectionTo.commit();

            return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), "PostgreSQL COPY");
        } else {
            return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), "The chunk has already been copied");
        }
    }

    private void writeValue(List<Column2Column> neededColumnsToDB,
                            ResultSet rs,
                            Chunk<?, ?, ?, ?> chunk,
                            PgBinaryWriter writer) throws SQLException, IOException {
        for (Column2Column entry : neededColumnsToDB) {
            String sourceColumn = entry.sourceColumn().columnName().replace("\"", "");
            String sourceType = entry.sourceColumn().columnType();
            String targetColumn = entry.targetColumn().columnName();
            String targetType = entry.targetColumn().columnType();
            int colIndex = rs.findColumn(sourceColumn);

            Object o = rs.getObject(colIndex);
            if (o == null) {
                writer.writeNull();
                continue;
            }

            switch (targetType) {
                case "json", "varchar", "bpchar", "char", "character": {
                    String s;
                    if (o instanceof org.postgresql.util.PGobject pgObject) {
                        s = pgObject.getValue();
                    } else {
                        s = o.toString();
                    }
                    writer.writeString(s != null ? s.replace("\u0000", "") : "");
                    break;
                }
                case "_text": {
                    String[] arr;
                    if (o instanceof java.sql.Array) {
                        arr = (String[]) ((java.sql.Array) o).getArray();
                    } else {
                        arr = (String[]) o;
                    }
                    writer.writeTextArray(arr);
                    break;
                }
                case "_varchar": {
                    String[] arr;
                    if (o instanceof java.sql.Array) {
                        arr = (String[]) ((java.sql.Array) o).getArray();
                    } else {
                        arr = (String[]) o;
                    }
                    writer.writeVarcharArray(arr);
                    break;
                }
                case "text": {
                    String s;
                    if (chunk.getSourceStorage().getClass().getName().equals(ORACLE_STORAGE_CLASS_NAME)) {
                        if (colIndex != 0 && rs.getMetaData().getColumnType(colIndex) == 2005) { // 2005 - Oracle CLOB
                            s = convertClobToString(rs, sourceColumn);
                        } else {
                            s = o.toString();
                        }
                    } else {
                        s = o.toString();
                    }
                    writer.writeString(s != null ? s.replace("\u0000", "") : "");
                    break;
                }
                case "jsonb": {
                    String s;
                    if (chunk.getSourceStorage().getClass().getName().equals(ORACLE_STORAGE_CLASS_NAME)) {
                        int columnIndex = getColumnIndexByColumnName(rs, sourceColumn.toUpperCase());
                        int columnType = rs.getMetaData().getColumnType(columnIndex);
                        s = switch (columnType) {
                            case 2005, 2011 -> convertClobToString(rs, sourceColumn).replace("\u0000", "");
                            default -> (o instanceof org.postgresql.util.PGobject pgo ?
                                    pgo.getValue() : o.toString()).replace("\u0000", "");
                        };
                    } else {
                        if (o instanceof org.postgresql.util.PGobject pgObject) {
                            s = pgObject.getValue();
                        } else {
                            s = (String) o;
                        }
                    }
                    writer.writeJsonb(s);
                    break;
                }
                case "money", "numeric", "decimal", "NUMBER": {
                    if (o instanceof BigDecimal bd) {
                        writer.writeNumeric(bd);
                    } else if (o instanceof Number num) {
                        writer.writeNumeric(new BigDecimal(num.toString()));
                    } else {
                        writer.writeNumeric(new BigDecimal(o.toString()));
                    }
                    break;
                }
                case "serial", "int4": {
                    if (o instanceof Number number) {
                        writer.writeInt(number.intValue());
                    } else {
                        writer.writeInt(Integer.parseInt(o.toString()));
                    }
                    break;
                }
                case "smallserial", "int2": {
                    if (o instanceof Number number) {
                        writer.writeShort(number.shortValue());
                    } else {
                        writer.writeShort(Short.parseShort(o.toString()));
                    }
                    break;
                }
                case "bigint", "int8": {
                    if (o instanceof Number number) {
                        writer.writeLong(number.longValue());
                    } else {
                        writer.writeLong(Long.parseLong(o.toString()));
                    }
                    break;
                }
                case "float4", "real" : {
                    System.out.println(o);
                    if (o instanceof Number number) {
                        writer.writeFloat(number.floatValue());
                    } else {
                        writer.writeFloat(Float.parseFloat(o.toString()));
                    }
                    break;
                }
                case "float8", "double precision": {
                    if (o instanceof Boolean bool) {
                        writer.writeBoolean(bool);
                    } else if (o instanceof Number number) {
                        writer.writeDouble(number.doubleValue());
                    } else {
                        writer.writeDouble(Double.parseDouble(o.toString()));
                    }
                    break;
                }
                case "bool": {
                    if (o instanceof Number number) {
                        writer.writeBoolean(number.intValue() > 0);
                    } else {
                        writer.writeBoolean(Boolean.parseBoolean(o.toString()));
                    }
                    break;
                }
                case "uuid":
                    UUID uuid = null;
                    try {
                        uuid = (UUID) o;
                    } catch (ClassCastException e) {
                        try {
                            uuid = UUID.fromString((String) o);
                        } catch (Exception e1) {
                            log.error("{}.{} : {} {} {}", chunk.getT2t().targetTable().getSchemaName(),
                                    chunk.getT2t().targetTable().getTableName(), targetColumn, o, getStackTrace(e1));
                        }
                    }
                    writer.writeUuid(uuid);
                    break;
                case "date": {
                    if (o instanceof java.sql.Date sqlDate) {
                        writer.writeDate(sqlDate.toLocalDate());
                    } else if (o instanceof LocalDate localDate) {
                        writer.writeDate(localDate);
                    }
                    break;
                }
                case "timestamp", "timestamp without time zone": {
                    LocalDateTime ldt = null;
                    try {
                        // Современный стандартный способ, поддерживаемый ojdbc8+ и pgjdbc
                        ldt = rs.getObject(sourceColumn, LocalDateTime.class);
                    } catch (Exception ex) {
                        // Резервный способ для старых версий драйверов
                        java.sql.Timestamp ts = rs.getTimestamp(sourceColumn);
                        if (ts != null) {
                            ldt = ts.toLocalDateTime();
                        }
                    }

                    if (ldt == null) {
                        writer.writeNull();
                    } else {
                        writer.writeTimestamp(ldt);
                    }
                    break;
                }

                case "timestamptz", "timestamp with time zone": {
                    OffsetDateTime odt = null;
                    try {
                        // Запрашиваем нативный Java 8 тип напрямую у драйвера (Oracle и Postgres это умеют)
                        odt = rs.getObject(sourceColumn, OffsetDateTime.class);
                    } catch (Exception ex) {
                        // Если ojdbc старый и упал, извлекаем через стандартный Timestamp
                        java.sql.Timestamp ts = rs.getTimestamp(sourceColumn);
                        if (ts != null) {
                            odt = ts.toInstant().atZone(java.time.ZoneId.systemDefault()).toOffsetDateTime();
                        }
                    }

                    if (odt == null) {
                        writer.writeNull();
                    } else {
                        writer.writeTimestampTz(odt);
                    }
                    break;
                }
                case "time", "time without time zone": {
                    if (o instanceof java.sql.Time sqlTime) {
                        writer.writeTime(sqlTime.toLocalTime());
                    } else if (o instanceof LocalTime localTime) {
                        writer.writeTime(localTime);
                    }
                    break;
                }
                case "bytea", "blob", "BINARY": {
                    byte[] bytes = null; // По умолчанию null

                    if (chunk.getSourceStorage().getClass().getName().equals(ORACLE_STORAGE_CLASS_NAME)) {
                        int columnIndex = getColumnIndexByColumnName(rs, sourceColumn.toUpperCase());
                        int columnType = rs.getMetaData().getColumnType(columnIndex);

                        bytes = switch (columnType) {
                            // RAW (-3), LONG RAW (-4)
                            case -3, -4 -> rs.getBytes(sourceColumn);
                            // BLOB (2004)
                            case 2004 -> convertBlobToBytes(rs, sourceColumn);
                            default -> rs.getBytes(sourceColumn);
                        };
                    } else {
                        if (o instanceof byte[]) {
                            bytes = (byte[]) o;
                        } else {
                            bytes = rs.getBytes(sourceColumn);
                        }
                    }
                    if (bytes == null) {
                        writer.writeNull();
                    } else {
                        writer.writeBytea(bytes);
                    }
                    break;
                }
                case "inet": {
                    java.net.InetAddress inetAddress;
                    if (o instanceof java.net.InetAddress) {
                        inetAddress = (java.net.InetAddress) o;
                    } else {
                        String ipStr = o.toString().trim();
                        inetAddress = java.net.InetAddress.getByName(ipStr);
                    }
                    writer.writeInet(inetAddress);
                    break;
                }
                case "hstore": {
                    java.util.Map<String, String> hstoreMap;
                    if (o instanceof java.util.Map) {
                        @SuppressWarnings("unchecked")
                        java.util.Map<String, String> castedMap = (java.util.Map<String, String>) o;
                        hstoreMap = castedMap;
                    } else {
                        hstoreMap = parseHstoreString(o.toString());
                    }
                    writer.writeHstore(hstoreMap);
                    break;
                }
                case "_bigint", "_int8": {
                    Long[] arr;
                    switch (o) {
                        case Array sqlArray -> {
                            Object innerArray = sqlArray.getArray();
                            if (innerArray instanceof long[] primitiveArr) {
                                arr = Arrays.stream(primitiveArr).boxed().toArray(Long[]::new);
                            } else if (innerArray instanceof Number[] numberArr) {
                                arr = Arrays.stream(numberArr).map(Number::longValue).toArray(Long[]::new);
                            } else {
                                arr = (Long[]) innerArray;
                            }
                        }
                        case long[] primitiveArr -> arr = Arrays.stream(primitiveArr).boxed().toArray(Long[]::new);
                        case Number[] numberArr ->
                                arr = Arrays.stream(numberArr).map(Number::longValue).toArray(Long[]::new);
                        default -> arr = (Long[]) o;
                    }
                    writer.writeLongArray(arr);
                    break;
                }
                case "_uuid": {
                    UUID[] arr;
                    if (o instanceof java.sql.Array sqlArray) {
                        arr = (UUID[]) sqlArray.getArray();
                    } else if (o instanceof String[] strArr) {
                        arr = java.util.Arrays.stream(strArr).map(s -> s != null ? UUID.fromString(s) : null).toArray(UUID[]::new);
                    } else {
                        arr = (UUID[]) o;
                    }
                    writer.writeUuidArray(arr);
                    break;
                }
                case "tstzrange": {
                    String rangeStr = null;

                    if (o instanceof org.postgresql.util.PGobject pgObject) {
                        rangeStr = pgObject.getValue();
                    } else {
                        rangeStr = o.toString();
                    }

                    if (rangeStr == null || rangeStr.equalsIgnoreCase("empty")) {
                        // Безопасно пишем флаг пустого диапазона через внутренний метод писателя
                        writer.writeEmptyRange();
                        break;
                    }

                    // Парсим строку Postgres формата: [lower,upper) или (lower,upper]
                    boolean lowerInclusive = rangeStr.startsWith("[");
                    boolean upperInclusive = rangeStr.endsWith("]");

                    // Отрезаем скобки
                    String content = rangeStr.substring(1, rangeStr.length() - 1);

                    // Сплит по запятой, но учитываем, что значения могут быть в кавычках: "2026-05-26 10:00:00+03"
                    String[] parts = content.split(",");

                    ZonedDateTime lowerBound = null;
                    ZonedDateTime upperBound = null;

                    // Парсим нижнюю границу
                    if (parts.length > 0 && !parts[0].trim().isEmpty() && !parts[0].contains("infinity")) {
                        String lowerStr = parts[0].replace("\"", "").trim();
                        // Заменяем пробел между датой и временем на 'T', если Postgres вернул формат "YYYY-MM-DD HH:MI:SS"
                        if (lowerStr.contains(" ") && !lowerStr.contains("T")) {
                            lowerStr = lowerStr.replace(" ", "T");
                        }
                        lowerBound = java.time.OffsetDateTime.parse(lowerStr).toZonedDateTime();
                    }

                    // Парсим верхнюю границу
                    if (parts.length > 1 && !parts[1].trim().isEmpty() && !parts[1].contains("infinity")) {
                        String upperStr = parts[1].replace("\"", "").trim();
                        if (upperStr.contains(" ") && !upperStr.contains("T")) {
                            upperStr = upperStr.replace(" ", "T");
                        }
                        upperBound = java.time.OffsetDateTime.parse(upperStr).toZonedDateTime();
                    }

                    // Отправляем разобранные границы в бинарный поток
                    writer.writeTstzRange(lowerBound, lowerInclusive, upperBound, upperInclusive);
                    break;
                }
                case "interval": {
                    int months = 0;
                    int days = 0;
                    long micros = 0;

                    if (chunk.getSourceStorage().getClass().getName().equals(ORACLE_STORAGE_CLASS_NAME)) {
                        int columnIndex = getColumnIndexByColumnName(rs, sourceColumn.toUpperCase());
                        int columnType = rs.getMetaData().getColumnType(columnIndex);
                        JDBCStorage jdbcSourceStorage = chunk.getSourceStorage().unwrap(JDBCStorage.class);

                        byte[] oracleBytes = rs.getBytes(sourceColumn);
                        if (oracleBytes == null) {
                            writer.writeNull();
                            break;
                        }

                        switch (columnType) {
                            // INTERVALYM
                            case -103: {
                                byte[] convertedBytes = jdbcSourceStorage.intervalYM2Interval((java.io.Serializable) o);
                                PgIntervalComponents comps = byteArrayYMToInterval(convertedBytes);
                                months = comps.months();
                                break;
                            }
                            // INTERVALDS
                            case -104: {
                                byte[] convertedBytes = jdbcSourceStorage.intervalDS2Interval((java.io.Serializable) o);
                                PgIntervalComponents comps = byteArrayDSToInterval(convertedBytes);
                                days = comps.days();
                                micros = comps.microseconds();
                                break;
                            }
                            default:
                                break;
                        }
                    } else {
                        if (o instanceof org.postgresql.util.PGInterval pgInterval) {
                            months = pgInterval.getYears() * 12 + pgInterval.getMonths();
                            days = pgInterval.getDays();
                            micros = (pgInterval.getHours() * 3600L + pgInterval.getMinutes() * 60L + (int) pgInterval.getSeconds()) * 1_000_000L
                                    + pgInterval.getMicroSeconds();
                        }
                    }

                    writer.writeInterval(months, days, micros);
                    break;
                }
                default:
                    if (chunk.getConfig().tryCharIfAny() != null) {
                        if (chunk.getConfig().tryCharIfAny().contains(targetColumn)) {
                            String s;
                            if (o instanceof org.postgresql.util.PGobject pgObject) {
                                s = pgObject.getValue();
                            } else {
                                s = (String) o;
                            }
                            writer.writeString(s != null ? s.replace("\u0000", "") : "");
                            break;
                        } else {
                            log.error("There is no handler for type: {}  for column: {}", targetType, targetColumn);
                        }
                    } else {
                        log.error("tryCharIfAny is NULL for Table: {}.{} Column: {} Type: {}",
                                chunk.getT2t().targetTable().getSchemaName(),
                                chunk.getT2t().targetTable().getTableName(),
                                targetType, targetColumn);
                        throw new RuntimeException("Unsupported type: " + targetType + " for column: " + targetColumn);
                    }
            }
        }
    }

    private boolean hasColumn(java.sql.ResultSet rs, String columnName) {
        try {
            rs.findColumn(columnName);
            return true;
        } catch (java.sql.SQLException e) {
            return false;
        }
    }


/*
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

            switch (targetType) {
                case "money": {
                    try {
                        Number s = fetchResultSet.getBigDecimal(sourceColumn);
                        if (s == null) {
                            row.setNumeric(targetColumn, null);
                            break;
                        }
                        row.setNumeric(targetColumn, s);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
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
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "bpchar":
                    try {
                        String string = fetchResultSet.getString(sourceColumn);
                        row.setText(targetColumn, string);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                    String s;
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
                        } else {
                            s = fetchResultSet.getString(sourceColumn);
                        }
                        row.setJsonb(targetColumn, s);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        log.error("{}.{} {} -> {}: {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
                        throw e;
                    }
                }
                case "numeric", "decimal", "NUMBER": {
                    try {
                        Object o = fetchResultSet.getObject(sourceColumn);
                        if (o == null) {
                            row.setNumeric(targetColumn, null);
                            break;
                        }
                        row.setNumeric(targetColumn, (Number) o);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} {} -> {}: {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
                        throw e;
                    }
                }
                case "float4" : {
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
                        log.error("{}.{} {} -> {}: {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
                        throw e;
                    }
                }
                case "float8", "double precision": {
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
                        log.error("{}.{} {} -> {}: {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
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
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        ZonedDateTime zonedDateTime =
                                ZonedDateTime.ofInstant(timestamp.toInstant(), ZoneId.of("UTC"));
                        row.setTimeStampTz(targetColumn, zonedDateTime);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                }
                case "date":
                    try {
                        java.sql.Date date = fetchResultSet.getDate(sourceColumn);
                        if (date == null) {
                            row.setDate(targetColumn, null);
                            break;
                        }
                        row.setDate(targetColumn, date.toLocalDate());
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        log.error("tstzrange : {}.{} - {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                                    interval = byteArrayYMToInterval(jdbcSourceStorage.intervalYM2Interval(intervalym));
                                    break;
                                // INTERVALDS
                                case -104:
                                    Serializable intervalds = (Serializable) fetchResultSet.getObject(sourceColumn);
                                    interval = byteArrayDSToInterval(jdbcSourceStorage.intervalDS2Interval(intervalds));
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
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        } else {
                            bytes = fetchResultSet.getBytes(sourceColumn);
                        }
                        row.setByteArray(targetColumn, bytes);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                                log.error("{}.{} : {} {} {}", chunk.getT2t().targetTable().getSchemaName(),
                                        chunk.getT2t().targetTable().getTableName(), targetColumn, o, getStackTrace(e1));
                            }
                        }
                        row.setUUID(targetColumn, uuid);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
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
                        log.error("{}.{} : {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), getStackTrace(e));
                        throw e;
                    }
                case "_bigint", "_int8": {
                    try {
                        Object o = fetchResultSet.getObject(sourceColumn);
                        if (o == null) {
                            row.setLong(targetColumn, null);
                            break;
                        }
                        List<Long> l = List.of((Long[]) fetchResultSet.getArray(sourceColumn).getArray());
                        row.setLongArray(targetColumn, l);
                        break;
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("{}.{} {} -> {}: {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(), sourceColumn, targetColumn, getStackTrace(e));
                        throw e;
                    }
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
                                streamApiService.closeSimpleRowWriter(writer);
//                                writer.close();
                                connectionTo.close();
                            }
                        } else {
                            log.error("tryCharIfAny is NULL for Table: {}.{} Column: {} Type: {}",
                                    chunk.getT2t().targetTable().getSchemaName(),
                                    chunk.getT2t().targetTable().getTableName(),
                                    targetType, targetColumn);
                            throw new RuntimeException("Unsupported type: " + targetType + " for column: " + targetColumn);
                        }
                    } catch (BinaryWriteFailedException | SQLException e) {
                        log.error("Table: {}.{} Column: {} Type: {}: {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(),
                                targetType, targetColumn, getStackTrace(e));
                        throw e;
                    }
            }
        }
    }
*/

    @Override
    public String buildFetchStatement(Config config, Table2Table<S> t2t) {
        List<String> asColumns = t2t.column2Columns()
                .stream()
                .filter(c2c -> c2c.sourceColumn() != null)
                .map(c2c -> c2c.sourceExpression() == null ? c2c.sourceColumn().columnName() : c2c.sourceExpression())
                .toList();
        List<String> asList = t2t.column2Columns()
                .stream()
                .map(Column2Column::asList)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .distinct()
                .toList();
        List<String> asSet = t2t.column2Columns()
                .stream()
                .map(Column2Column::asSet)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .distinct()
                .toList();
        List<KV> asMap = t2t.column2Columns()
                .stream()
                .map(Column2Column::asMap)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .distinct()
                .toList();
        List<String> asUDT = t2t.column2Columns()
                .stream()
                .map(Column2Column::asUDT)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .distinct()
                .toList();
//        asMap.forEach(kv -> log.info("asMap: {} -> {}", kv.key(), kv.value()));
        Set<String> set = new HashSet<>(asColumns);
        set.addAll(asList);
        set.addAll(asSet);
        set.addAll(asMap.stream().map(KV::key).toList());
        set.addAll(asMap.stream().map(KV::value).toList());
        set.addAll(asUDT);
        List<String> finalList = set.stream().toList();
        String columnToColumn = String.join(", ", finalList);
        return PGKeywords.SELECT + " " +
                columnToColumn + " " +
                (t2t.ttlColumn() == null ? "" : ( ", " + t2t.ttlColumn().defaultValue() + " as " + t2t.ttlColumn().columnName() + " ")) +
                (t2t.timestampColumn() == null ? "" : ( ", " + t2t.timestampColumn().defaultValue() + " as " + t2t.timestampColumn().columnName() + " ")) +
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
    public void fulfillChunks(List<Config> configs,
                              boolean sync,
                              int required,
                              String tableName) throws SQLException {
        Connection connection = getConnection();
        createChunkTable(connection, sync, tableName);
        for (Config config : configs) {
            long reltuples = 0;
            long relpages = 0;
            long max_end_page;
            Table<S> table = configToTable(config.fromSchemaName(), config.fromTableName());

            PreparedStatement preparedStatement = connection.prepareStatement(SQL_NUMBER_OF_TUPLES);
            preparedStatement.setString(1, table.getSchemaName().toLowerCase());
            preparedStatement.setString(2, table.getFinalTableName(false));
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                reltuples = resultSet.getLong("reltuples");
                relpages = resultSet.getLong("relpages");
                char relkind = resultSet.getString("relkind").charAt(0);
                if (relkind == 'p') {
                    throw new RuntimeException("Partitioned tables are not supported: "
                            + config.fromSchemaName() + '.' + config.fromTableName());
                }
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
    public void createGlobalOutbox(String tableName) throws SQLException {
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
    public void dropChunkTable(List<Config> configs, boolean sync, String tableName) {
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
        }
    }

    @Override
    public boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk, String tableName) {
        try {
            Connection connectionTo = (Connection) chunk.getTargetSession();
            PreparedStatement ps = connectionTo.prepareStatement(DML_SELECT_OUTBOX_TABLE.replace("$tableName", tableName));
            ps.setInt(1, (int) chunk.getId());
            ResultSet rs = ps.executeQuery();
            boolean result = rs.next();
            rs.close();
            ps.close();
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertProcessedChunkInfo(Chunk<?, ?, ?, ?> chunk, String tableName) {
        try {
            Connection connection = (Connection) chunk.getTargetSession();
            PreparedStatement ps = connection.prepareStatement(DML_INSERT_OUTBOX_TABLE.replace("$tableName", tableName));
            ps.setInt(1, (int) chunk.getId());
            ps.setString(2, chunk.getConfig().fromTaskName());
            ps.setLong(3, chunk.getCopied());
            long r = ps.executeUpdate();
            ps.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Table<S> configToTable(String schemaName, String tableName) {
        return new PGTable<>(schemaName, tableName);
    }

    @Override
    public void enrichTable(Table<S> table) throws SQLException {
        S session = getPoolConnection();
        table.enrichTable(session);
        session.close();
    }

    @Override
    public void enrichTable(Table<S> sourceTable, Table<S> targetTable) throws SQLException {
        S session = getPoolConnection();
        if (!targetTable.enrichTable(session)) {
            targetTable.setOptions(sourceTable.getOptions());
            targetTable.setColumns(sourceTable.getColumns());
            targetTable.create(session);
        } else {
            targetTable.enrichTable(session);
        }
        session.close();
    }

    @Override
    public <W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        Connection connectionTo = chunk.getTargetSession();

        Map<String, Column> columnToColumnMap = chunk.getT2t().column2Columns()
                .stream()
                .collect(Collectors.toMap(el -> el.sourceColumn().columnName(), Column2Column::targetColumn));

        PGConnection pgConnection = connectionTo.unwrap(PGConnection.class);

        String[] columnNames = columnToColumnMap
                .values()
                .stream()
                .map(Column::columnName)
                .toList()
                .toArray(String[]::new);
        String[] cNames = Arrays.copyOf(columnNames, columnNames.length);
        SimpleRowWriter.Table table =
                new SimpleRowWriter.Table(chunk.getT2t().targetTable().getSchemaName(),
                        chunk.getT2t().targetTable().getFinalTableName(true), cNames);
        SimpleRowWriter writer = new SimpleRowWriter(table, pgConnection);
        return (W) writer;
    }

    @Override
    public <V, W> void insertColumnValue(List<ColumnValue<V>> columnValues,
                                         Chunk<K, T, S, R> chunk,
                                         W writer) throws SQLException {
        Consumer<SimpleRow> simpleRowConsumer =
                s -> consume(s, columnValues, chunk);

        ((SimpleRowWriter) writer).startRow(simpleRowConsumer);
    }

    private <V> void consume(SimpleRow s, List<ColumnValue<V>> columnValues, Chunk<K, T, S, R> chunk) {
        for (ColumnValue<V> columnValue : columnValues) {
            String targetColumnName = columnValue.targetColumn().columnName();
            String targetType = columnValue.targetColumn().columnType();
            V value = columnValue.value();
            switch (targetType) {
                case "int", "serial", "int4": {
                    if (value != null) {
                        s.setInteger(targetColumnName, (Integer) value);
                    } else {
                        s.setInteger(targetColumnName, null);
                    }
                    break;
                }
                case "smallserial", "int2": {
                    if (value != null) {
                        if (value instanceof Short) {
                            s.setShort(targetColumnName, (Short) value);
                        } else {
                            s.setShort(targetColumnName, ((Integer) value).shortValue());
                        }
                    } else {
                        s.setShort(targetColumnName, null);
                    }
                    break;
                }
                case "bigint", "int8": {
                    if (value != null) {
                        if (value instanceof Long) {
                            s.setLong(targetColumnName, (Long) value);
                        } else {
                            s.setLong(targetColumnName, ((Number) value).longValue());
                        }
                    } else {
                        s.setLong(targetColumnName, null);
                    }
                    break;
                }
                case "numeric", "decimal": {
                    if (value != null) {
                        s.setNumeric(targetColumnName, (BigDecimal) value);
                    } else {
                        s.setNumeric(targetColumnName, null);
                    }
                    break;
                }
                case "float4": {
                    if (value != null) {
                        s.setFloat(targetColumnName, (Float) value);
                    } else {
                        s.setFloat(targetColumnName, null);
                    }
                    break;
                }
                case "float8", "double precision": {
                    if (value != null) {
                        if (value instanceof Double) {
                            s.setDouble(targetColumnName, (Double) value);
                        } else {
                            Float f = (Float) value;
                            s.setDouble(targetColumnName, f.doubleValue());
                        }
                    } else {
                        s.setDouble(targetColumnName, null);
                    }
                    break;
                }
                case "json", "varchar": {
                    if (value != null) {
                        s.setVarChar(targetColumnName, (String) value);
                    } else {
                        s.setVarChar(targetColumnName, null);
                    }
                    break;
                }
                case "_text": {
                    if (value != null) {
                        if (value instanceof List) {
                            s.setTextArray(targetColumnName, (List<String>) value);
                        } else if (value instanceof Set) {
                            s.setTextArray(targetColumnName, (Set<String>) value);
                        }
                    } else {
                        s.setTextArray(targetColumnName, null);
                    }
                    break;
                }
                case "text", "bpchar": {
                    if (value != null) {
                        s.setText(targetColumnName, (String) value);
                    } else {
                        s.setText(targetColumnName, null);
                    }
                    break;
                }
                case "jsonb": {
                    if (value != null) {
                        s.setJsonb(targetColumnName, (String) value);
                    } else {
                        s.setJsonb(targetColumnName, null);
                    }
                    break;
                }
                case "time": {
                    if (value != null) {
                        s.setTime(targetColumnName, (LocalTime) value);
                    } else {
                        s.setTime(targetColumnName, null);
                    }
                    break;
                }
                case "timestamp": {
                    if (value != null) {
                        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant((Instant) value, ZoneId.of("UTC"));
                        s.setTimeStamp(targetColumnName, LocalDateTime.ofInstant((Instant) value, zonedDateTime.getZone()));
                    } else {
                        s.setTimeStamp(targetColumnName, null);
                    }
                    break;
                }
                case "date": {
                    if (value != null) {
                        s.setDate(targetColumnName, (LocalDate) value);
                    } else {
                        s.setDate(targetColumnName, null);
                    }
                    break;
                }
                case "bytea": {
                    if (value != null) {
                        ByteBuffer buffer = (ByteBuffer) value;
                        s.setByteArray(targetColumnName, buffer.array());
                    } else {
                        s.setByteArray(targetColumnName, null);
                    }
                    break;
                }
                case "bool": {
                    if (value != null) {
                        s.setBoolean(targetColumnName, (Boolean) value);
                    } else {
                        s.setBoolean(targetColumnName, null);
                    }
                    break;
                }
                case "inet": {
                    if (value != null) {
                        InetAddress inetAddress = (InetAddress) value;
                        if (inetAddress instanceof Inet4Address inet4Address) {
                            s.setInet4Addr(targetColumnName, inet4Address);
                        } else {
                            Inet6Address inet6Address = (Inet6Address) inetAddress;
                            s.setInet6Addr(targetColumnName, inet6Address);
                        }
                    } else {
                        s.setInet4Addr(targetColumnName, null);
                    }
                    break;
                }
                case "uuid": {
                    if (value != null) {
                        s.setUUID(targetColumnName, (UUID) value);
                    } else {
                        s.setUUID(targetColumnName, null);
                    }
                    break;
                }
                default:
                    try {
                        if (chunk.getConfig().tryCharIfAny() != null) {
                            if (chunk.getConfig().tryCharIfAny().contains(targetColumnName)) {
                                if (value == null) {
                                    s.setText(targetColumnName, null);
                                    break;
                                }
                                String str = value.toString();
                                s.setText(targetColumnName, str.replaceAll("\u0000", ""));
                                break;
                            } else {
                                log.error("There is no handler for type: {}  for column: {}", targetType, targetColumnName);
                            }
                        } else {
                            log.error("tryCharIfAny is NULL for Table: {}.{} Column: {} Type: {}",
                                    chunk.getT2t().targetTable().getSchemaName(),
                                    chunk.getT2t().targetTable().getTableName(),
                                    targetType, targetColumnName);
                            throw new RuntimeException("Unsupported type: " + targetType + " for column: " + targetColumnName);
                        }
                    } catch (BinaryWriteFailedException e) {
                        log.error("Table: {}.{} Column: {} Type: {}: {}", chunk.getT2t().targetTable().getSchemaName(), chunk.getT2t().targetTable().getTableName(),
                                targetType, targetColumnName, getStackTrace(e));
                        throw e;
                    }
            }
        }
    }

    @Override
    public <W> void closeWriter(W writer, Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        Connection connectionTo = chunk.getTargetSession();
        ((SimpleRowWriter) writer).close();
        connectionTo.commit();
    }
}
