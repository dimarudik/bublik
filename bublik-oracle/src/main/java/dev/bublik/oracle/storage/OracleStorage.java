package dev.bublik.oracle.storage;

import dev.bublik.core.model.*;
import dev.bublik.oracle.model.OraChunkRowId;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.constants.PGKeywords;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import dev.bublik.oracle.model.OraTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.*;
import java.util.*;

import static dev.bublik.core.util.Utils.getStackTrace;
import static dev.bublik.oracle.constants.SQLConstants.*;

public class OracleStorage extends JDBCStorage {
    private static final Logger log = LoggerFactory.getLogger(OracleStorage.class);

    public OracleStorage(StorageClass storageClass,
                         ConnectionProperty connectionProperty,
                         Table outboxTable) throws SQLException {
        super(storageClass, connectionProperty, outboxTable);
    }

    private OracleStorage(Builder builder) {
        super(builder);
    }

    public static class Builder extends JDBCStorage.Builder<OracleStorage, Builder> {

        public Builder(DataSource dataSource) {
            super(dataSource);
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public OracleStorage build() {
            validate();
            return new OracleStorage(this);
        }
    }

    @Override
    public <W extends Serializable> byte[] intervalYM2Interval(W intervalym) {
        byte[] bytes;
        bytes = ((INTERVALYM)intervalym).toBytes();
        return bytes;
    }

    @Override
    public <W extends Serializable> byte[] intervalDS2Interval(W intervalds) {
        byte[] bytes;
        bytes = ((INTERVALDS)intervalds).toBytes();
        return bytes;
    }

    @Override
    public <K, T, S extends AutoCloseable, R>  LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        return null;
    }

    @Override
    public void fulfillChunks(List<Config> configs, boolean synz, int rows) throws SQLException {
        Connection connection = this.getPoolConnection();
        for (Config config : configs) {
            try {
                CallableStatement dropTask = connection.prepareCall(PLSQL_DROP_TASK);
                dropTask.setString(1, config.fromTaskName());
                dropTask.execute();
                dropTask.close();
                log.info("Dropped task {}", config.fromTaskName());
            } catch (SQLException e) {
                log.warn("Task {} does not exist", config.fromTaskName());
            }
        }
        for (Config config : configs) {
            try {
                CallableStatement createTask = connection.prepareCall(PLSQL_CREATE_TASK);
                createTask.setString(1, config.fromTaskName());
                createTask.execute();
                createTask.close();
            } catch (SQLException e) {
                log.error("{}", getStackTrace(e));
                throw e;
            }
        }

        for (Config config : configs) {
            try {
                Table table = configToTable(config.fromSchemaName(), config.fromTableName());
                CallableStatement createChunk = connection.prepareCall(PLSQL_CREATE_CHUNK);
                createChunk.setString(1, config.fromTaskName());
                createChunk.setString(2, table.getSchemaName().toUpperCase());
                createChunk.setString(3, table.getFinalTableName(false));
                createChunk.setInt(4, rows);
                createChunk.execute();
                createChunk.close();
                log.info("Created chunks for task {}", config.fromTaskName());
            } catch (SQLException e) {
                log.error("{}", getStackTrace(e));
                throw e;
            }
        }
        log.info("ROWID chunks created successfully");
        connection.close();
    }

    @Override
    public void preChecks(List<Config> configs) throws SQLException {

    }

    @Override
    public void createChunkTable() throws SQLException {

    }

    @Override
    public void dropChunkTable(List<Config> configs) throws SQLException {
        Connection connection = this.getPoolConnection();
        for (Config config : configs) {
            try {
                CallableStatement dropTask = connection.prepareCall(PLSQL_DROP_TASK);
                dropTask.setString(1, config.fromTaskName());
                dropTask.execute();
                dropTask.close();
                log.info("Dropped task {}", config.fromTaskName());
            } catch (SQLException e) {
                log.warn("Task {} does not exist", config.fromTaskName());
            }
        }
        connection.close();
    }

    @Override
    public void createGlobalOutbox() throws SQLException {

    }

    @Override
    public void insertProcessedChunkInfo(Chunk<?, ?, ?, ?> chunk) throws SQLException {

    }

    @Override
    public void dropOutboxTable(boolean sync) throws SQLException {

    }

    @Override
    public Chunk<?, ?, ?, ?> getChunk(ResultSet rs, TableMigrationContext ctx, Storage targetStorage) throws SQLException {
        String status = rs.getString("status");
        return new OraChunkRowId<>(
                rs.getInt("chunk_id"),
                rs.getRowId("start_rowid"),
                rs.getRowId("end_rowid"),
                ctx.config(),
                ctx.t2t(),
                ChunkStatus.valueOf(status),
                ctx.fetchQuery(),
                this,
                targetStorage,
                ctx.orderByClause());
    }

/*
    @Override
    public List<Chunk<?, ?, ?, ?>> getChunkList(List<Config> configs,
                                                Storage targetStorage) throws SQLException {
        List<Chunk<?, ?, ?, ?>> chunkHashList = new ArrayList<>();
        for (Config config : configs) {
            Table sourceTable = this.configToTable(config.fromSchemaName(), config.fromTableName());
            Table targetTable = targetStorage.configToTable(config.toSchemaName(), config.toTableName());

            this.enrichTable(sourceTable);
            targetStorage.enrichTable(sourceTable, targetTable);

            List<Column2Column> c2c = getColumn2Column(sourceTable, targetTable, config);
            Table2Table t2t = getTable2Table(sourceTable, targetTable, c2c, config);

            String sql = buildStartEndOfChunk(config, sourceTable);
            log.debug("Query of chunks for table {}.{}: {}", t2t.sourceTable().getSchemaName(), t2t.sourceTable().getTableName(), sql);
            String fetchQuery = buildFetchStatement(config, t2t);
            String orderByClause = targetTable.buildOrderBy(config);

            log.info("Fetch query: {} {}", fetchQuery, orderByClause);
            Connection sourceSession = this.getPoolConnection();
            PreparedStatement ps = sourceSession.prepareStatement(sql);
            ps.setString(1, config.fromTaskName());
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String status = resultSet.getString("status");
                Chunk<?, ?, ?, ?> chunk = new OraChunkRowId<>(
                        resultSet.getInt("chunk_id"),
                        resultSet.getRowId("start_rowid"),
                        resultSet.getRowId("end_rowid"),
                        config,
                        t2t,
                        ChunkStatus.valueOf(status),
                        fetchQuery,
                        this,
                        targetStorage,
                        orderByClause);
                chunkHashList.add(chunk);
            }
            resultSet.close();
            ps.close();
            sourceSession.close();
        }
        return chunkHashList;
    }
*/

    @Override
    public Table2Table getTable2Table(Table sourceTable,
                                      Table targetTable,
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
        return new Table2Table(sourceTable, targetTable, c2c, ttlColumn, timestampColumn);
    }

    @Override
    public Table getDefaultSourceOutboxTable() {
        return new OraTable.Builder("UNDEFINED","UNDEFINED").build();
    }

    @Override
    public Table getDefaultTargetOutboxTable() {
        return new OraTable.Builder("UNDEFINED","UNDEFINED").build();
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
            column2Column.addAll(matchColumns(sourceTable, targetTable));
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
                        key = kv.key() + " as K_" + targetColumnName + k++;
                    }
                    String value;
                    if (isColumnNameWithAsConstruction(kv.value())) {
                        value = kv.value();
                    } else {
                        value = kv.value() + " as V_" + targetColumnName + v++;
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
        return  "select chunk_id, start_rowid, end_rowid, start_id, end_id, task_name, status " +
                "from user_parallel_execute_chunks where status <> 'PROCESSED' and task_name = ? " +
                (config.fromTaskWhereClause() == null ? " " : " and " + config.fromTaskWhereClause())
                + " and rownum <= 200 ";
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
        return  PGKeywords.SELECT + " /* bublik */ " +
                (config.fetchHintClause() == null ? "" : config.fetchHintClause()) + " " +
                columnToColumn + " " +
                (t2t.ttlColumn() == null ? "" : ( ", " + t2t.ttlColumn().defaultValue() + " as \"" + t2t.ttlColumn().columnName() + "\" ")) +
                (t2t.timestampColumn() == null ? "" : ( ", " + t2t.timestampColumn().defaultValue() + " as \"" + t2t.timestampColumn().columnName() + "\" ")) +
                PGKeywords.FROM + " " +
                config.fromSchemaName() +
                "." +
                config.fromTableName() + " " +
                (config.fromTableAlias() == null ? "" : config.fromTableAlias()) + " " +
                (config.fromTableAdds() == null ? "" : config.fromTableAdds()) + " " +
                PGKeywords.WHERE +
                (config.fetchWhereClause() == null ? " " : " ( " + config.fetchWhereClause() + " ) and ") +
                (config.fromTableAlias() == null ? "" : config.fromTableAlias() + ".") +
                "rowid between ? and ?";
    }

    @Override
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?, ?, ?> chunk) {
        return Map.of();
    }

    @Override
    public void createPrimaryKeys() {

    }

    @Override
    public void createIndexes() {

    }

    @Override
    public void createForeignKeys() {

    }

    @Override
    public void createUniqueConstraints() {

    }

    @Override
    public Table configToTable(String schemaName, String tableName) {
        return new OraTable(schemaName, tableName);
    }

    @Override
    public void enrichTable(Table sourceTable, Table targetTable) throws SQLException {

    }

    @Override
    public void enrichTable(Table table) throws SQLException {
        Connection session = getPoolConnection();
        table.enrichTable(session);
        session.close();
    }
}
