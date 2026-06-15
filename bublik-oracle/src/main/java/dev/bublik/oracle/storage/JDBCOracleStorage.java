package dev.bublik.oracle.storage;

import dev.bublik.core.model.*;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.constants.PGKeywords;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import dev.bublik.oracle.model.OraChunk;
import dev.bublik.oracle.model.OraTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.*;
import java.util.*;

import static dev.bublik.core.util.Utils.getStackTrace;
import static dev.bublik.oracle.constants.SQLConstants.*;

public class JDBCOracleStorage<K extends Integer, T extends RowId, S extends Connection, R extends ResultSet> extends JDBCStorage<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(JDBCOracleStorage.class);

    public JDBCOracleStorage(StorageClass storageClass, ConnectionProperty connectionProperty) throws SQLException {
        super(storageClass, connectionProperty);
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
    public LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        return null;
    }

    @Override
    public Map.Entry<String, Long> getSystemChangeNumberWithTrxId() throws SQLException {
        return null;
    }

    @Override
    public void fulfillChunks(List<Config> configs, boolean synz, int rows, String tableName) throws SQLException {
        Connection connection = getConnection();
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
        log.info("Ctid chunks created successfully");
    }

    @Override
    public void dropChunkTable(List<Config> configs, boolean sync, String tableName) throws SQLException {
        Connection connection = getConnection();
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
    }

    @Override
    public void createGlobalOutbox(String tableName) throws SQLException {

    }

    @Override
    public void dropOutboxTable(boolean sync, String tableName) throws SQLException {

    }

    @Override
    public List<Chunk<K, T, S, R>> getChunkList(List<Config> configs, String chunkTable, Storage<K, T, S, R> targetStorage) throws SQLException {
        List<Chunk<K, T, S, R>> chunkHashList = new ArrayList<>();
        for (Config config : configs) {
            Table<S> sourceTable = this.configToTable(config.fromSchemaName(), config.fromTableName());
            Table<S> targetTable = targetStorage.configToTable(config.toSchemaName(), config.toTableName());
            this.enrichTable(sourceTable);
            targetStorage.enrichTable(sourceTable, targetTable);
            List<Column2Column> c2c = getColumn2Column(sourceTable, targetTable, config);
            Table2Table<S> t2t = getTable2Table(sourceTable, targetTable, c2c, config);
            S sourceSession = this.getPoolConnection();
            String sql = buildStartEndOfChunk(config, chunkTable, sourceTable);
            log.debug("SQL to fetch metadata of chunks: {}", sql);
            PreparedStatement ps = sourceSession.prepareStatement(sql);
            ps.setString(1, config.fromTaskName());
            ResultSet resultSet = ps.executeQuery();
            String fetchQuery = buildFetchStatement(config, t2t);
            String orderByClause = targetTable.buildOrderBy(config);
            log.info("Fetch query: {} {}", fetchQuery, orderByClause);
            while (resultSet.next()) {
                String status = resultSet.getString("status");
                Chunk<K, T, S, R> chunk = new OraChunk<>(
                        (K)Integer.valueOf(resultSet.getInt("chunk_id")),
                        (T)resultSet.getRowId("start_rowid"),
                        (T)resultSet.getRowId("end_rowid"),
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
            column2Column.addAll(matchColumns(sourceTable, targetTable));
        }
        if (config.columnToColumn() != null) {
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
    public String buildStartEndOfChunk(Config config, String chunkTable, Table<S> sourceTable) {
        return  "select chunk_id, start_rowid, end_rowid, start_id, end_id, task_name, status " +
                "from user_parallel_execute_chunks where status <> 'PROCESSED' and task_name = ? " +
                (config.fromTaskWhereClause() == null ? " " : " and " + config.fromTaskWhereClause());

    }

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
        Set<String> set = new HashSet<>(asColumns);
        set.addAll(asList);
        set.addAll(asSet);
        set.addAll(asMap.stream().map(KV::key).toList());
        set.addAll(asMap.stream().map(KV::value).toList());
        set.addAll(asUDT);
        List<String> finalList = set.stream().toList();
        String columnToColumn = String.join(", ", finalList);
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

/*
    @Override
    public void enrichSourceTables(Connection connection) {
        Map<Table<S>, Table<S>> tables = getTables();
        try {
            Connection sourceConnection = getPoolConnection();
            for (Map.Entry<Table<S>, Table<S>> entry : tables.entrySet()) {
                Table<S> sourceTable = entry.getKey();
                List<Column> allSourceColumns = sourceTable.getAllColumns((S)sourceConnection);
                sourceTable.setColumns(allSourceColumns);
            }
            sourceConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }
*/

    @Override
    public Table<S> configToTable(String schemaName, String tableName) {
        return new OraTable<>(schemaName, tableName);
    }

    @Override
    public void enrichTable(Table<S> sourceTable, Table<S> targetTable) throws SQLException {

    }

    @Override
    public void enrichTable(Table<S> table) throws SQLException {
        S session = getPoolConnection();
        table.enrichTable(session);
        session.close();
    }

    @Override
    public void initCache(List<Config> configs) throws SQLException {
        log.info("Cache is not implemented for this storage");
    }
}
