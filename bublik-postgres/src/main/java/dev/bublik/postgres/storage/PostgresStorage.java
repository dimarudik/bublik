package dev.bublik.postgres.storage;

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
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.util.*;

import static dev.bublik.core.constants.CLassConstants.ORACLE_STORAGE_CLASS_NAME;
import static dev.bublik.core.util.ColumnUtil.*;
import static dev.bublik.core.util.Utils.getStackTrace;
import static dev.bublik.postgres.constants.SQLConstants.*;
import static dev.bublik.postgres.util.ColumnUtil.*;

public class PostgresStorage extends JDBCStorage {
    private static final Logger log = LoggerFactory.getLogger(PostgresStorage.class);

    public PostgresStorage(StorageClass storageClass,
                           ConnectionProperty connectionProperty,
                           Table outboxTable) throws SQLException {
        super(storageClass, connectionProperty, outboxTable);
    }

    private PostgresStorage(Builder builder) {
        super(builder);
    }

    public static class Builder extends JDBCStorage.Builder<PostgresStorage, Builder> {

        public Builder(DataSource dataSource) {
            super(dataSource);
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public PostgresStorage build() {
            validate();
            return new PostgresStorage(this);
        }
    }

    @Override
    public Chunk<?, ?, ?, ?> getChunk(ResultSet rs, TableMigrationContext ctx, Storage targetStorage) throws SQLException {
        String status = rs.getString("status");
        return new PGChunk<>(
                rs.getInt("chunk_id"),
                rs.getLong("start_page"),
                rs.getLong("end_page"),
                ctx.config(),
                ctx.t2t(),
                ChunkStatus.valueOf(status),
                ctx.fetchQuery(),
                this,
                targetStorage,
                ctx.orderByClause());
    }

    @Override
    public Table2Table getTable2Table(Table sourceTable,
                                      Table targetTable,
                                      List<Column2Column> c2c,
                                      Config config) {
        Column ttlColumn = null;
        Column timestampColumn = null;
        if (config.withTTL() != null) {
            ttlColumn = Column.builder()
                    .columnPosition(-1)
                    .columnName("_ttl")
                    .columnType("int")
                    .defaultValue(config.withTTL())
                    .build();
        }
        if (config.timestamp() != null) {
            timestampColumn = Column.builder()
                    .columnPosition(-1)
                    .columnName("_timestamp")
                    .columnType("int")
                    .defaultValue(config.timestamp())
                    .build();
/*
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
*/
        }
        return new Table2Table(sourceTable, targetTable, c2c, ttlColumn, timestampColumn);
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
    public List<Column2Column> getColumn2Column(Table sourceTable, Table targetTable, Config config) {
        List<Column2Column> column2Column = new ArrayList<>();
        if ((config.columnToColumn() == null || config.columnToColumn().isEmpty()) &&
                (config.expressionToColumn() == null || config.expressionToColumn().isEmpty()) &&
                (config.asList() == null || config.asList().isEmpty())) {
            if (sourceTable.getClass() == targetTable.getClass()) {
                sourceTable.getColumns().forEach(c -> column2Column.add(new Column2Column(c, c)));
            } else {
                column2Column.addAll(matchColumns(sourceTable, targetTable));
            }
        }
        if ((config.columnToColumn() != null && !config.columnToColumn().isEmpty()) &&
                (config.avroSchema() == null || config.avroSchema().isEmpty())) {
            for (Map.Entry<String,String> entry : config.columnToColumn().entrySet()) {
                Column sourceColumn = sourceTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes()
                                .equalsIgnoreCase(entry.getKey().replace("\"", "")))
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
        if ((config.expressionToColumn() != null && !config.expressionToColumn().isEmpty()) &&
                (config.avroSchema() == null || config.avroSchema().isEmpty())) {
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
        int avroFieldPosition = 1;
        if ((config.columnToColumn() != null && !config.columnToColumn().isEmpty()) &&
                (config.avroSchema() != null && !config.avroSchema().isEmpty())) {
            for (Map.Entry<String, String> entry : config.columnToColumn().entrySet()) {
                Column sourceColumn = sourceTable.getColumns().stream()
                        .filter(c -> c.getNameWithoutQuotes()
                                .equalsIgnoreCase(entry.getKey().replace("\"", "")))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(entry.getKey() + " not found in source table " +
                                sourceTable.getSchemaName() + "." + sourceTable.getTableName()));
                Column targetColumn = columnFromAvro(config.avroSchema(), entry.getValue(), avroFieldPosition++);
                column2Column.add(new Column2Column(sourceColumn, targetColumn, null));
            }
        }
        if ((config.expressionToColumn() != null && !config.expressionToColumn().isEmpty()) &&
                (config.avroSchema() != null && !config.avroSchema().isEmpty())) {
            for (Map.Entry<String, String> entry : config.expressionToColumn().entrySet()) {
                Column targetColumn = columnFromAvro(config.avroSchema(), entry.getValue(), avroFieldPosition++);
                column2Column.add(new Column2Column(targetColumn, targetColumn, entry.getKey()));
            }
        }
        if (config.asList() != null && !config.asList().isEmpty()) {
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
        if (config.asSet() != null && !config.asSet().isEmpty()) {
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
        if (config.asMap() != null && !config.asMap().isEmpty()) {
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
        if (config.asUDT() != null && !config.asUDT().isEmpty()) {
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
    public String buildStartEndOfChunk(Config config, Table sourceTable) {
        return "select chunk_id, uuid, start_page, end_page, task_name, status from " +
                getOutboxTable().tableToString() + " where " +
                "task_name = ? " +
//                "schema_name = ? and table_name = ? and task_name = ? " +
                " and status in ('ASSIGNED', 'UNASSIGNED', 'PROCESSED_WITH_ERROR') "
                + " limit 200 ";
    }

    @Override
    public <K, T, S extends AutoCloseable, R> LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName)
            throws SQLException, SourceSQLException, TargetSQLException {
        if (chunk.getSourceStorage() instanceof JDBCStorage) {
            ResultSet fetchResultSet = (ResultSet) chunk.getResultSet();

            if (fetchResultSet == null) {
                return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), "NO ROWS FETCH");
            }

            Connection connectionTo = (Connection) chunk.getTargetSession();
            try {
                return fetchAndCopy(fetchResultSet, chunk);
            } catch (SQLException e) {
                connectionTo.rollback();
                throw e;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        } else {
            return chunk.getSourceStorage().transfer(chunk, tableName);
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
                    Table sourceTable = chunk.getT2t().sourceTable();
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

    private LogMessage fetchAndCopy(ResultSet fetchResultSet,
                                    Chunk<?, ?, ?, ?> chunk) throws SQLException, SourceSQLException, IOException {
        int recordCount = 0;
        Connection connectionTo = (Connection) chunk.getTargetSession();

        if (!isChunkProcessed(chunk)) {
            List<Column2Column> columnToColumnList = chunk.getT2t().column2Columns();
            int[] sourceColumnIndices = new int[columnToColumnList.size()];
            int[] sourceColumnTypes = new int[columnToColumnList.size()];
            ResultSetMetaData meta = fetchResultSet.getMetaData();
            for (int i = 0; i < columnToColumnList.size(); i++) {
                String sourceColumnName = columnToColumnList.get(i).sourceColumn().columnName().replace("\"", "");
                int index = fetchResultSet.findColumn(sourceColumnName);
                sourceColumnIndices[i] = index;
                sourceColumnTypes[i] = meta.getColumnType(index);
            }
            List<String> columnNames = columnToColumnList.stream().map(Column2Column::targetColumn).map(Column::columnName).toList();

            String tableNameWithSchema = chunk.getT2t().targetTable().getSchemaName() + "." +
                    chunk.getT2t().targetTable().getFinalTableName(true);
            String sqlCopy = "COPY " + tableNameWithSchema + " (" + String.join(", ", columnNames) + ") FROM STDIN BINARY";

            int pgStreamBufferSize = 1024 * 1024;
            int javaBufferSize = 64 * 1024;
            PGConnection pgConnection = connectionTo.unwrap(PGConnection.class);

            try (PGCopyOutputStream os = new PGCopyOutputStream(pgConnection, sqlCopy, pgStreamBufferSize);
                 PgBinaryWriter writer = new PgBinaryWriter(os, javaBufferSize)) {
                while (fetchResultSet.next()) {
                    writer.startRow((short) columnNames.size());
                    writeValue(columnToColumnList, sourceColumnIndices, sourceColumnTypes, fetchResultSet, chunk, writer);
                    recordCount++;
                }
            }

            chunk.setCopied(recordCount);
            insertProcessedChunkInfo(chunk);
            connectionTo.commit();

            return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), "PostgreSQL COPY");
        } else {
            return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), "The chunk has already been copied");
        }
    }

    private void writeValue(List<Column2Column> columnToColumnList,
                            int[] sourceColumnIndices,
                            int[] sourceColumnTypes,
                            ResultSet rs,
                            Chunk<?, ?, ?, ?> chunk,
                            PgBinaryWriter writer) throws SQLException, IOException {
        boolean isOracle = chunk.getSourceStorage().getClass().getName().equals(ORACLE_STORAGE_CLASS_NAME);
        for (int i = 0; i < columnToColumnList.size(); i++) {
            Column2Column entry = columnToColumnList.get(i);
            String sourceColumn = entry.sourceColumn().columnName().replace("\"", "");
            String targetColumn = entry.targetColumn().columnName().replace("\"", "");
            String targetType = entry.targetColumn().columnType();

            int colIndex = sourceColumnIndices[i];
            int sourceSqlType = sourceColumnTypes[i];

            Object value = rs.getObject(colIndex);
            if (value == null) {
                writer.writeNull();
                continue;
            }

            switch (targetType) {
                case "json", "varchar", "bpchar", "char", "character": {
                    String s;
                    if (value instanceof org.postgresql.util.PGobject pgObject) {
                        s = pgObject.getValue();
                    } else if (value instanceof java.sql.SQLXML sqlXml) {
                        s = sqlXml.getString();
                    } else {
                        s = value.toString();
                    }
                    writer.writeString(s != null ? s.replace("\u0000", "") : "");
                    break;
                }

                case "xml": {
                    String s;
                    if (value instanceof java.sql.SQLXML sqlXml) {
                        s = sqlXml.getString();
                    } else if (value instanceof org.postgresql.util.PGobject pgObject) {
                        s = pgObject.getValue();
                    } else {
                        s = value.toString();
                    }
                    writer.writeXml(s != null ? s.replace("\u0000", "") : "");
                    break;
                }

                case "_text": {
                    String[] arr;
                    if (value instanceof java.sql.Array) {
                        arr = (String[]) ((java.sql.Array) value).getArray();
                    } else {
                        arr = (String[]) value;
                    }
                    writer.writeTextArray(arr);
                    break;
                }

                case "_varchar": {
                    String[] arr;
                    if (value instanceof java.sql.Array) {
                        arr = (String[]) ((java.sql.Array) value).getArray();
                    } else {
                        arr = (String[]) value;
                    }
                    writer.writeVarcharArray(arr);
                    break;
                }

                case "text": {
                    String s;
                    if (isOracle) {
//                        if (colIndex != 0 && rs.getMetaData().getColumnType(colIndex) == 2005) { // 2005 - Oracle CLOB
                        if (sourceSqlType == 2005) {
                            s = convertClobToString(rs, sourceColumn);
                        } else {
                            s = value.toString();
                        }
                    } else {
                        s = value.toString();
                    }
                    writer.writeString(s != null ? s.replace("\u0000", "") : "");
                    break;
                }

                case "jsonb": {
                    String s;
                    if (isOracle) {
//                        int columnIndex = getColumnIndexByColumnName(rs, sourceColumn.toUpperCase());
//                        int columnType = rs.getMetaData().getColumnType(columnIndex);
                        s = switch (sourceSqlType) {
                            case 2005, 2011 -> convertClobToString(rs, sourceColumn).replace("\u0000", "");
                            default -> (value instanceof org.postgresql.util.PGobject pgo ?
                                    pgo.getValue() : value.toString()).replace("\u0000", "");
                        };
                    } else {
                        if (value instanceof org.postgresql.util.PGobject pgObject) {
                            s = pgObject.getValue();
                        } else {
                            s = (String) value;
                        }
                    }
                    writer.writeJsonb(s);
                    break;
                }

                case "money", "numeric", "decimal", "NUMBER": {
                    if (value instanceof BigDecimal bd) {
                        writer.writeNumeric(bd);
                    } else if (value instanceof Number num) {
                        writer.writeNumeric(new BigDecimal(num.toString()));
                    } else {
                        writer.writeNumeric(new BigDecimal(value.toString()));
                    }
                    break;
                }

                case "int", "serial", "int4": {
                    if (value instanceof Number number) {
                        writer.writeInt(number.intValue());
                    } else {
                        writer.writeInt(Integer.parseInt(value.toString()));
                    }
                    break;
                }

                case "smallserial", "int2": {
                    if (value instanceof Number number) {
                        writer.writeShort(number.shortValue());
                    } else {
                        writer.writeShort(Short.parseShort(value.toString()));
                    }
                    break;
                }

                case "bigint", "int8": {
                    if (value instanceof Number number) {
                        writer.writeLong(number.longValue());
                    } else {
                        writer.writeLong(Long.parseLong(value.toString()));
                    }
                    break;
                }

                case "float4", "real" : {
                    if (value instanceof Number number) {
                        writer.writeFloat(number.floatValue());
                    } else {
                        writer.writeFloat(Float.parseFloat(value.toString()));
                    }
                    break;
                }

                case "float8", "double precision": {
                    if (value instanceof Boolean bool) {
                        writer.writeBoolean(bool);
                    } else if (value instanceof Number number) {
                        writer.writeDouble(number.doubleValue());
                    } else {
                        writer.writeDouble(Double.parseDouble(value.toString()));
                    }
                    break;
                }

                case "bool": {
                    if (value instanceof Number number) {
                        writer.writeBoolean(number.intValue() > 0);
                    } else {
                        writer.writeBoolean(Boolean.parseBoolean(value.toString()));
                    }
                    break;
                }

                case "uuid": {
                    UUID uuid = null;
                    try {
                        uuid = (UUID) value;
                    } catch (ClassCastException e) {
                        try {
                            uuid = UUID.fromString((String) value);
                        } catch (Exception e1) {
                            log.error("{}.{} : {} {} {}", chunk.getT2t().targetTable().getSchemaName(),
                                    chunk.getT2t().targetTable().getTableName(), targetColumn, value, getStackTrace(e1));
                        }
                    }
                    writer.writeUuid(uuid);
                    break;
                }

                case "date": {
                    switch (value) {
                        case Timestamp timestamp -> writer.writeDate(timestamp.toLocalDateTime().toLocalDate());
                        case Date sqlDate -> writer.writeDate(sqlDate.toLocalDate());
                        case LocalDate localDate -> writer.writeDate(localDate);
                        default ->
                                throw new SQLException("Cannot map " + value.getClass().getName() + " to PostgreSQL DATE");
                    }
                    break;
                }

                case "timestamp", "timestamp without time zone": {
                    LocalDateTime ldt = null;
                    try {
                        ldt = rs.getObject(sourceColumn, LocalDateTime.class);
                    } catch (Exception ex) {
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
                        odt = rs.getObject(sourceColumn, OffsetDateTime.class);
                    } catch (Exception ex) {
                        Timestamp ts = rs.getTimestamp(sourceColumn);
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
                    if (value instanceof java.sql.Time sqlTime) {
                        writer.writeTime(sqlTime.toLocalTime());
                    } else if (value instanceof LocalTime localTime) {
                        writer.writeTime(localTime);
                    }
                    break;
                }

                case "bytea", "blob", "BINARY": {
                    byte[] bytes = null; // По умолчанию null

                    if (isOracle) {
                        bytes = switch (sourceSqlType) {
                            // RAW (-3), LONG RAW (-4)
                            case -3, -4 -> rs.getBytes(sourceColumn);
                            // BLOB (2004)
                            case 2004 -> convertBlobToBytes(rs, sourceColumn);
                            default -> rs.getBytes(sourceColumn);
                        };
                    } else {
                        if (value instanceof byte[]) {
                            bytes = (byte[]) value;
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
                    if (value instanceof java.net.InetAddress) {
                        inetAddress = (java.net.InetAddress) value;
                    } else {
                        String ipStr = value.toString().trim();
                        inetAddress = java.net.InetAddress.getByName(ipStr);
                    }
                    writer.writeInet(inetAddress);
                    break;
                }

                case "hstore": {
                    java.util.Map<String, String> hstoreMap;
                    if (value instanceof java.util.Map) {
                        @SuppressWarnings("unchecked")
                        java.util.Map<String, String> castedMap = (java.util.Map<String, String>) value;
                        hstoreMap = castedMap;
                    } else {
                        hstoreMap = parseHstoreString(value.toString());
                    }
                    writer.writeHstore(hstoreMap);
                    break;
                }

                case "_bigint", "_int8": {
                    Long[] arr;
                    switch (value) {
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
                        default -> arr = (Long[]) value;
                    }
                    writer.writeLongArray(arr);
                    break;
                }

                case "_uuid": {
                    UUID[] arr;
                    if (value instanceof java.sql.Array sqlArray) {
                        arr = (UUID[]) sqlArray.getArray();
                    } else if (value instanceof String[] strArr) {
                        arr = java.util.Arrays.stream(strArr).map(s -> s != null ? UUID.fromString(s) : null).toArray(UUID[]::new);
                    } else {
                        arr = (UUID[]) value;
                    }
                    writer.writeUuidArray(arr);
                    break;
                }

                case "tstzrange": {
                    String rangeStr = null;

                    if (value instanceof org.postgresql.util.PGobject pgObject) {
                        rangeStr = pgObject.getValue();
                    } else {
                        rangeStr = value.toString();
                    }

                    if (rangeStr == null || rangeStr.equalsIgnoreCase("empty")) {
                        writer.writeEmptyRange();
                        break;
                    }

                    boolean lowerInclusive = rangeStr.startsWith("[");
                    boolean upperInclusive = rangeStr.endsWith("]");

                    String content = rangeStr.substring(1, rangeStr.length() - 1);

                    String[] parts = content.split(",");

                    ZonedDateTime lowerBound = null;
                    ZonedDateTime upperBound = null;

                    if (parts.length > 0 && !parts[0].trim().isEmpty() && !parts[0].contains("infinity")) {
                        String lowerStr = parts[0].replace("\"", "").trim();
                        if (lowerStr.contains(" ") && !lowerStr.contains("T")) {
                            lowerStr = lowerStr.replace(" ", "T");
                        }
                        lowerBound = java.time.OffsetDateTime.parse(lowerStr).toZonedDateTime();
                    }

                    if (parts.length > 1 && !parts[1].trim().isEmpty() && !parts[1].contains("infinity")) {
                        String upperStr = parts[1].replace("\"", "").trim();
                        if (upperStr.contains(" ") && !upperStr.contains("T")) {
                            upperStr = upperStr.replace(" ", "T");
                        }
                        upperBound = java.time.OffsetDateTime.parse(upperStr).toZonedDateTime();
                    }

                    writer.writeTstzRange(lowerBound, lowerInclusive, upperBound, upperInclusive);
                    break;
                }

                case "interval": {
                    int months = 0;
                    int days = 0;
                    long micros = 0;

                    if (isOracle) {
                        JDBCStorage jdbcSourceStorage = chunk.getSourceStorage().unwrap(JDBCStorage.class);

                        byte[] oracleBytes = rs.getBytes(sourceColumn);
                        if (oracleBytes == null) {
                            writer.writeNull();
                            break;
                        }

                        switch (sourceSqlType) {
                            // INTERVALYM
                            case -103: {
                                byte[] convertedBytes = jdbcSourceStorage.intervalYM2Interval((java.io.Serializable) value);
                                PgIntervalComponents comps = byteArrayYMToInterval(convertedBytes);
                                months = comps.months();
                                break;
                            }
                            // INTERVALDS
                            case -104: {
                                byte[] convertedBytes = jdbcSourceStorage.intervalDS2Interval((java.io.Serializable) value);
                                PgIntervalComponents comps = byteArrayDSToInterval(convertedBytes);
                                days = comps.days();
                                micros = comps.microseconds();
                                break;
                            }
                            default:
                                break;
                        }
                    } else {
                        if (value instanceof org.postgresql.util.PGInterval pgInterval) {
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
                            if (value instanceof org.postgresql.util.PGobject pgObject) {
                                s = pgObject.getValue();
                            } else {
                                s = (String) value;
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

    @Override
    public String buildFetchStatement(Config config, Table2Table t2t) {
        List<Column2Column> sortedColumn2Columns = t2t.getSortedColumn2ColumnByTargetColumnPosition();
        List<String> asColumns = new ArrayList<>(sortedColumn2Columns
                .stream()
                .filter(c2c -> c2c.sourceColumn() != null)
                .map(c2c -> c2c.sourceExpression() == null ? c2c.sourceColumn().columnName() : c2c.sourceExpression())
                .toList());
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
        Set<String> set = new HashSet<>();
        set.addAll(asList);
        set.addAll(asSet);
        set.addAll(asMap.stream().map(KV::key).toList());
        set.addAll(asMap.stream().map(KV::value).toList());
        set.addAll(asUDT);
        asColumns.addAll(set);
        String columnToColumn = String.join(", ", asColumns);
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
        Map<Table, Table> tables = getTables();
        try (Connection targetConnection = getPoolConnection()) {
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table targetTable = entry.getValue();
                targetTable.createPrimaryKey(targetConnection);
            }
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void createIndexes() {
        Map<Table, Table> tables = getTables();
        try (Connection targetConnection = getPoolConnection()){
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table taregtTable = entry.getValue();
                taregtTable.createIndexes(targetConnection);
            }
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void createForeignKeys() {
        Map<Table, Table> tables = getTables();
        try (Connection targetConnection = getPoolConnection()) {
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table taregtTable = entry.getValue();
                taregtTable.createForeignKeys(targetConnection);
            }
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void createUniqueConstraints() {
        Map<Table, Table> tables = getTables();
        try (Connection targetConnection = getPoolConnection()) {
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table taregtTable = entry.getValue();
                taregtTable.createUniqueConstraints(targetConnection);
            }
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public Map.Entry<String,Long> getSystemChangeNumberWithTrxId() throws SQLException {
        try (Statement st = ((Connection)getPoolConnection()).createStatement();
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
    public void preChecks(List<Config> configs) throws SQLException {
        try (Connection connection = getPoolConnection()) {
            for (Config config : configs) {
                Table table = configToTable(config.fromSchemaName(), config.fromTableName());
                TableAttrs tableAttrs = getTableAttrs(connection, table);
                if (tableAttrs.relkind() == 'p') {
                    throw new RuntimeException("Partitioned tables are not supported: "
                            + table.getSchemaName() + '.' + table.getTableName());
                }
            }
        }
    }

    @Override
    public void fulfillChunks(List<Config> configs,
                              boolean sync,
                              int required) throws SQLException {
        try (Connection connection = getPoolConnection()) {
            for (Config config : configs) {
                long reltuples = 0;
                long relpages = 0;
                long max_end_page;
                Table table = configToTable(config.fromSchemaName(), config.fromTableName());

                try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_NUMBER_OF_TUPLES)) {
                    preparedStatement.setString(1, table.getSchemaName().toLowerCase());
                    preparedStatement.setString(2, table.getFinalTableName(false));
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            reltuples = resultSet.getLong("reltuples");
                            relpages = resultSet.getLong("relpages");
                        }
                    }
                }

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
                insertCtidChunksV2(connection, config, table, 0, relpages, pagesInChunk,
                        ChunkStatus.UNASSIGNED, required, getOutboxTable().tableToString());

                max_end_page = getMaxEndPageOfChunks(connection, config, getOutboxTable().tableToString());

                // всавка последних чанков
                if (heap_blks_total > max_end_page) {
                    insertCtidChunksV2(connection, config, table, max_end_page, heap_blks_total,
                            pagesInChunk, ChunkStatus.UNASSIGNED, required, getOutboxTable().tableToString());
                }
            }
            connection.commit();
            log.info("Chunk table {} fulfilled successfully", getOutboxTable().tableToString());
        }
    }

    record TableAttrs (long reltuples, long relpages, char relkind){}

    private TableAttrs getTableAttrs(Connection connection, Table table) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_NUMBER_OF_TUPLES);) {
            preparedStatement.setString(1, table.getSchemaName().toLowerCase());
            preparedStatement.setString(2, table.getFinalTableName(false));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return new TableAttrs(
                            resultSet.getLong("reltuples"),
                            resultSet.getLong("relpages"),
                            resultSet.getString("relkind").charAt(0));
                } else {
                    throw new RuntimeException("Table " + table.getSchemaName() + "." + table.getFinalTableName(false) + " not found");
                }
            }
        }
    }

    @Override
    public void createGlobalOutbox() throws SQLException {
        if (getOutboxTable() == null) setOutboxTable(new DummyTable("public", "_outbox"));
        try (Connection connection = getPoolConnection();
             Statement createTable = connection.createStatement()) {
            createTable.executeUpdate(DDL_CREATE_OUTBOX_TABLE.replace("$tableName",
                    getOutboxTable().tableToString()));
            connection.commit();
            log.info("Outbox table {} created successfully", getOutboxTable().tableToString());
        } catch (SQLException e) {
            log.warn("Outbox table {} already exists", getOutboxTable().tableToString());
        }
    }

    @Override
    public void createChunkTable() throws SQLException {
        if (getOutboxTable() == null) setOutboxTable(new DummyTable("public", "_chunk"));
        try (Connection connection = this.getPoolConnection();
             Statement createTable = connection.createStatement()) {
            createTable.executeUpdate(DDL_CREATE_CHUNK_TABLE.replace("$tableName",
                    getOutboxTable().tableToString()));
            connection.commit();
            log.info("Chunk table {} created successfully", getOutboxTable().tableToString());
        } catch (SQLException e) {
            log.error("Chunk table {} already exists", getOutboxTable().tableToString());
            throw e;
        }
    }

    @Override
    public void dropChunkTable(List<Config> configs) throws SQLException {
        try (Connection connection = this.getPoolConnection();
             Statement dropTable = connection.createStatement()) {
            dropTable.executeUpdate(DDL_DROP_CHUNK_TABLE.replace("$tableName", getOutboxTable().tableToString()));
            connection.commit();
        }
    }

    @Override
    public void dropOutboxTable(boolean sync) throws SQLException {
        try (Connection connection = getPoolConnection();
             Statement dropTable = connection.createStatement()){
            dropTable.executeUpdate(DDL_DROP_OUTBOX_TABLE.replace("$tableName",
                    getOutboxTable().tableToString()));
            connection.commit();
        }
    }

    public boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk) {
        Connection connectionTo = (Connection) chunk.getTargetSession();
        try (PreparedStatement ps = connectionTo.prepareStatement(DML_SELECT_OUTBOX_TABLE.replace(
                     "$tableName", getOutboxTable().tableToString()))) {
            ps.setInt(1, (int) chunk.getId());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertProcessedChunkInfo(Chunk<?, ?, ?, ?> chunk) throws SQLException {
        try {
            Connection connection = (Connection) chunk.getTargetSession();
            PreparedStatement ps = connection.prepareStatement(DML_INSERT_OUTBOX_TABLE.replace(
                    "$tableName", getOutboxTable().tableToString()));
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
    public Table configToTable(String schemaName, String tableName) {
        return new PGTable(schemaName, tableName);
    }

    @Override
    public void enrichTable(Table table) throws SQLException {
        try (Connection session = getPoolConnection()) {
            table.enrichTable(session);
        }
    }

    @Override
    public void enrichTable(Table sourceTable, Table targetTable) throws SQLException {
        try (Connection session = getPoolConnection()) {
            if (!targetTable.enrichTable(session) && sourceTable.getClass().equals(targetTable.getClass())) {
                targetTable.setOptions(sourceTable.getOptions());
                targetTable.setColumns(sourceTable.getColumns());
                targetTable.create((Connection) session);
            } else {
                targetTable.enrichTable(session);
            }
        }
    }

    @Override
    public  <K, T, S extends AutoCloseable, R, W> W getWriter(Chunk<K, T, S, R> chunk,
                            String tableName) throws SQLException {
        Connection connectionTo = (Connection) chunk.getTargetSession();
        List<Column2Column> columnToColumnMap = chunk.getT2t().column2Columns();
        List<String> columnNames = columnToColumnMap.stream().map(Column2Column::targetColumn).map(Column::columnName).toList();

        String tableNameWithSchema = chunk.getT2t().targetTable().getSchemaName() + "." +
                chunk.getT2t().targetTable().getFinalTableName(true);
        String sql = "COPY " + tableNameWithSchema + " (" + String.join(", ", columnNames) + ") FROM STDIN BINARY";

        int pgStreamBufferSize = 1024 * 1024;
        int javaBufferSize = 64 * 1024;
        PGConnection pgConnection = connectionTo.unwrap(PGConnection.class);
        PGCopyOutputStream os = new PGCopyOutputStream(pgConnection, sql, pgStreamBufferSize);
        try {
            PgBinaryWriter writer = new PgBinaryWriter(os, javaBufferSize);
            return (W) writer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <K, T, S extends AutoCloseable, R, V> void insertColumnValue(List<ColumnValue<V>> columnValues,
                                         Chunk<K, T, S, R> chunk) throws SQLException {
        try {
            PgBinaryWriter pgBinaryWriter = (PgBinaryWriter) chunk.getWriter();
            pgBinaryWriter.startRow((short) columnValues.size());
            writeValue(pgBinaryWriter, columnValues, chunk);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private <K, T, S extends AutoCloseable, R, V> void writeValue(PgBinaryWriter writer,
                                                                  List<ColumnValue<V>> columnValues,
                                                                  Chunk<K, T, S, R> chunk) throws IOException, SQLException {
        for (ColumnValue<V> columnValue : columnValues) {
            String targetColumnName = columnValue.targetColumn().columnName();
            String targetType = columnValue.targetColumn().columnType();
            V value = columnValue.value();

            if (value == null) {
                writer.writeNull();
                continue;
            }

            switch (targetType) {
                case "int", "serial", "int4": {
                    if (value instanceof Number number) {
                        writer.writeInt(number.intValue());
                    } else {
                        writer.writeInt(Integer.parseInt(value.toString()));
                    }
                    break;
                }
                case "smallserial", "int2": {
                    if (value instanceof Number number) {
                        writer.writeShort(number.shortValue());
                    } else {
                        writer.writeShort(Short.parseShort(value.toString()));
                    }
                    break;
                }
                case "bigint", "int8": {
                    if (value instanceof Number number) {
                        writer.writeLong(number.longValue());
                    } else {
                        writer.writeLong(Long.parseLong(value.toString()));
                    }
                    break;
                }
                case "money", "numeric", "decimal", "NUMBER": {
                    if (value instanceof BigDecimal bd) {
                        writer.writeNumeric(bd);
                    } else if (value instanceof Number num) {
                        writer.writeNumeric(new BigDecimal(num.toString()));
                    } else {
                        writer.writeNumeric(new BigDecimal(value.toString()));
                    }
                    break;
                }
                case "float4", "real" : {
                    if (value instanceof Number number) {
                        writer.writeFloat(number.floatValue());
                    } else {
                        writer.writeFloat(Float.parseFloat(value.toString()));
                    }
                    break;
                }
                case "float8", "double precision": {
                    if (value instanceof Boolean bool) {
                        writer.writeBoolean(bool);
                    } else if (value instanceof Number number) {
                        writer.writeDouble(number.doubleValue());
                    } else {
                        writer.writeDouble(Double.parseDouble(value.toString()));
                    }
                    break;
                }
                case "_text", "_varchar": {
                    String[] arr = null;

                    switch (value) {
                        case Array sqlArray -> arr = (String[]) sqlArray.getArray();
                        case Collection<?> col -> arr = col.stream()
                                .map(item -> item != null ? item.toString() : null)
                                .toArray(String[]::new);
                        case String[] strArr -> arr = strArr;
                        default -> {
                        }
                    }
                    if (arr == null || arr.length == 0) {
                        writer.writeNull();
                    } else {
                        if (targetType.equals("_text")) {
                            writer.writeTextArray(arr);
                        } else {
                            writer.writeVarcharArray(arr);
                        }
                    }
                    break;
                }
                case "json", "varchar", "bpchar", "char", "character": {
                    String s;
                    if (value instanceof org.postgresql.util.PGobject pgObject) {
                        s = pgObject.getValue();
                    } else {
                        s = value.toString();
                    }
                    writer.writeString(s != null ? s.replace("\u0000", "") : "");
                    break;
                }
                case "text": {
                    String s;
                    if (value instanceof org.postgresql.util.PGobject pgObject) {
                        s = pgObject.getValue();
                    } else {
                        s = value.toString();
                    }
                    writer.writeString(s != null ? s.replace("\u0000", "") : "");
                    break;
                }
                case "jsonb": {
                    String s;
                    if (value instanceof org.postgresql.util.PGobject pgObject) {
                        s = pgObject.getValue();
                    } else {
                        s = (String) value;
                    }
                    writer.writeJsonb(s);
                    break;
                }
                case "time", "time without time zone": {
                    if (value instanceof java.sql.Time sqlTime) {
                        writer.writeTime(sqlTime.toLocalTime());
                    } else if (value instanceof LocalTime localTime) {
                        writer.writeTime(localTime);
                    }
                    break;
                }
/*
                case "timestamp", "timestamp without time zone": {
                    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant((Instant) value, ZoneId.of("UTC"));
                    writer.writeTimestamp(zonedDateTime.toLocalDateTime());
                    break;
                }
*/
                case "timestamp", "timestamp without time zone": {
                    if (value instanceof Instant instant) {
                        LocalDateTime localDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
                        writer.writeTimestamp(localDateTime);
                    } else if (value instanceof LocalDateTime ldt) {
                        writer.writeTimestamp(ldt);
                    }
                    break;
                }
                case "timestamptz", "timestamp with time zone": {
                    if (value instanceof Instant instant) {
                        OffsetDateTime odt = instant.atOffset(java.time.ZoneOffset.UTC);
                        writer.writeTimestampTz(odt);
                    }
                    break;
                }
                case "date": {
                    if (value instanceof java.sql.Date sqlDate) {
                        writer.writeDate(sqlDate.toLocalDate());
                    } else if (value instanceof LocalDate localDate) {
                        writer.writeDate(localDate);
                    }
                    break;
                }
                case "bytea", "blob", "BINARY": {
                    ByteBuffer buffer = (ByteBuffer) value;
                    writer.writeBytea(buffer.array());
                    break;
                }
                case "bool": {
                    if (value instanceof Number number) {
                        writer.writeBoolean(number.intValue() > 0);
                    } else {
                        writer.writeBoolean(Boolean.parseBoolean(value.toString()));
                    }
                    break;
                }
                case "inet": {
                    java.net.InetAddress inetAddress;
                    if (value instanceof java.net.InetAddress) {
                        inetAddress = (java.net.InetAddress) value;
                    } else {
                        String ipStr = value.toString().trim();
                        inetAddress = java.net.InetAddress.getByName(ipStr);
                    }
                    writer.writeInet(inetAddress);
                    break;
                }
                case "uuid":
                    UUID uuid = null;
                    try {
                        uuid = (UUID) value;
                    } catch (ClassCastException e) {
                        try {
                            uuid = UUID.fromString((String) value);
                        } catch (Exception e1) {
                            log.error("{}.{} : {} {} {}", chunk.getT2t().targetTable().getSchemaName(),
                                    chunk.getT2t().targetTable().getTableName(), targetColumnName, value, getStackTrace(e1));
                        }
                    }
                    writer.writeUuid(uuid);
                    break;
                default:
                    if (chunk.getConfig().tryCharIfAny() != null) {
                        if (chunk.getConfig().tryCharIfAny().contains(targetColumnName)) {
                            String s;
                            if (value instanceof org.postgresql.util.PGobject pgObject) {
                                s = pgObject.getValue();
                            } else {
                                s = (String) value;
                            }
                            writer.writeString(s != null ? s.replace("\u0000", "") : "");
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
            }
        }
    }

    @Override
    public <K, T, S extends AutoCloseable, R> void closeWriter(Chunk<K, T, S, R> chunk, String tableName) {
        Connection connectionTo = (Connection) chunk.getTargetSession();
        try {
            PgBinaryWriter w = (PgBinaryWriter) chunk.getWriter();
            DataOutputStream os = w.getOut();
            w.close();
            os.close();
            connectionTo.commit();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

/*
    public static Config.Builder builder() {
        return new Config.Builder();
    }
*/
}
