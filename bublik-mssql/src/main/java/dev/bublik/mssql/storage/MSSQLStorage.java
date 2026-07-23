package dev.bublik.mssql.storage;

import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.constants.PGKeywords;
import dev.bublik.core.model.*;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import dev.bublik.mssql.model.MSSQLChunk;
import dev.bublik.mssql.model.MSSQLTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static dev.bublik.core.constants.Constants.FROM;
import static dev.bublik.core.constants.Constants.TO;
import static dev.bublik.mssql.constants.SQLConstants.*;

public class MSSQLStorage extends JDBCStorage {
    private static final Logger log = LoggerFactory.getLogger(MSSQLStorage.class);

    public MSSQLStorage(DataSource dataSource) {
        super(dataSource, null);
    }

    public MSSQLStorage(DataSource dataSource,
                        Table outboxTable) {
        super(dataSource, outboxTable);
    }

    public MSSQLStorage(DataSource dataSource,
                        int threadCount,
                        Table outboxTable) {
        super(dataSource, threadCount, outboxTable);
    }

    protected MSSQLStorage(DataSource dataSource,
                           ConnectionProperty connectionProperty,
                           Table outboxTable) {
        super(dataSource, connectionProperty, outboxTable);
    }

    public MSSQLStorage(StorageClass storageClass,
                        ConnectionProperty connectionProperty,
                        Table outboxTable) throws SQLException {
        super(storageClass, connectionProperty, outboxTable);
    }

    @Override
    public void createPrimaryKeys() {

    }

    @Override
    public void createUniqueConstraints() {

    }

    @Override
    public void createIndexes() {

    }

    @Override
    public void createForeignKeys() {

    }

    @Override
    public void fulfillChunks(List<Config> configs, boolean sync, int rows) throws SQLException {
        Connection connection = this.getPoolConnection();
        for (Config config : configs) {
            MSSQLTable sourceTable = (MSSQLTable) configToTable(config.fromSchemaName(), config.fromTableName());
            List<Column> clusteringKey = sourceTable.getClusteringKeyColumns(connection);
            sourceTable.setClusteringKey(clusteringKey);
            checkIfClusteringKeyIsNotEmpty(clusteringKey);
            checkIfClusteringKeyHasNullableColumns(clusteringKey);
            sourceTable.setClusteringKey(clusteringKey);
            createChunkExtTable(connection, sync, sourceTable);
            insertChunkTable(connection, sourceTable, config, rows);
        }
        dropSequence(connection, sync);
        connection.close();
        log.info("Chunk table {} created successfully", getOutboxTable().tableToString());
    }

    @Override
    public void preChecks(List<Config> configs) throws SQLException {

    }

    private void insertChunkTable(Connection connection,
                                  MSSQLTable sourceTable,
                                  Config config,
                                  int rows) throws SQLException {
        String clusteringKeyColumnListByComma = getClusteringKeyColumnListByComma(sourceTable);
        String clusteringKeyColumnAscDescListByComma = getClusteringKeyColumnAscDescListByComma(sourceTable);
        String leadColumns = getColumnsChunkLimits(sourceTable.getClusteringKey(), FROM, TO);
        String fromToColumnsByComma = getStringFromToClusteringKey(sourceTable, ", ", "");
        String insertChunkExtSql =
                DML_INSERT_EXT_CHUNKS
                        .replace("$schemaName", schemaName())
                        .replace("$tableName", sourceTable.getSchemaName() + "." + sourceTable.getTableName())
                        .replace("$columns", clusteringKeyColumnListByComma)
                        .replace("$colsAscDesc", clusteringKeyColumnAscDescListByComma)
                        .replace("$leadColumns", leadColumns)
                        .replace("$extTableName", sourceTable.getTableName())
                        .replace("$fromToColumns", fromToColumnsByComma);
        log.info("\n{}", insertChunkExtSql);
        PreparedStatement insertExtTable = connection.prepareStatement(insertChunkExtSql);
        insertExtTable.setInt(1, rows);
        insertExtTable.executeUpdate();
        insertExtTable.close();
        String insertChunkSql = DML_INSERT_CHUNKS
                .replace("$schemaName", schemaName())
                .replace("$tableName", getOutboxTable().getTableName())
                .replace("$extTableName", sourceTable.getTableName());
        PreparedStatement insertTable = connection.prepareStatement(insertChunkSql);
        insertTable.setString(1, "bublik");
        insertTable.setString(2, "_ext_" + sourceTable.getTableName());
        insertTable.setString(3, config.fromSchemaName());
        insertTable.setString(4, config.fromTableName());
        insertTable.setInt(5, rows);
        insertTable.setString(6, config.fromTaskName());
        insertTable.executeUpdate();
        insertTable.close();
        connection.commit();
    }

    private String getColumnsChunkLimits(List<Column> clusteringKey, String startWord, String endWord) {
        String startColumns = clusteringKey
                .stream()
                .map(Column::columnName)
                .map(col -> String.format("%s AS " + startWord + "%s", col, col))
                .collect(Collectors.joining(", "));
        String orderPart = "ORDER BY RowNum";
        String endColumns = clusteringKey.stream()
                .map(Column::columnName)
                .map(col -> String.format("LEAD(%s) OVER (%s) AS " + endWord + "%s", col, orderPart, col))
                .collect(Collectors.joining(", "));
        return startColumns + ", " + endColumns;
    }

    private void checkIfClusteringKeyIsNotEmpty(List<Column> clusteringKey) {
        clusteringKey.stream().findFirst().orElseThrow(() -> new RuntimeException("Clustering key is empty"));
    }

    private void checkIfClusteringKeyHasNullableColumns(List<Column> clusteringKey) {
        clusteringKey
                .stream()
                .filter(Column::nullable)
                .findFirst()
                .ifPresent(column -> {throw new RuntimeException("Clustering key has nullable column: " + column.columnName());});
    }

    private void createSequence(Connection connection, boolean sync) throws SQLException {
        Statement createTable = connection.createStatement();
        createTable.executeUpdate(DDL_CREATE_CHUNK_SEQ.replace("$schemaName", schemaName()));
        createTable.close();
        connection.commit();
    }

    private void dropSequence(Connection connection, boolean sync) throws SQLException {
        Statement createTable = connection.createStatement();
        createTable.executeUpdate(DDL_DROP_CHUNK_SEQ.replace("$schemaName", schemaName()));
        createTable.close();
        connection.commit();
    }

    private void createSchema(Connection connection, boolean sync) throws SQLException {
        Statement createTable = connection.createStatement();
        createTable.executeUpdate(DDL_CREATE_SCHEMA.replace("$schemaName", schemaName()));
        createTable.close();
        connection.commit();
    }

    private void createChunkExtTable(Connection connection, boolean sync, MSSQLTable table) throws SQLException {
        Statement createTable = connection.createStatement();
        String columnList = getStringFromToClusteringKeyWithType(table, ", ");
        String sql = DDL_CREATE_CHUNK_EXT_TABLE
                .replace("$schemaName", schemaName())
                .replace("$extTableName", table.getTableName())
                .replace("$columns", columnList);
        log.info("Creating chunk ext table: {}", sql);
        createTable.executeUpdate(sql);
        createTable.close();
        connection.commit();
    }

    private String getClusteringKeyColumnListByComma(MSSQLTable sourceTable) {
        return String.join(", ", sourceTable.getClusteringKey().stream().map(Column::columnName).toList());
    }

    private String getClusteringKeyColumnAscDescListByComma(MSSQLTable sourceTable) {
        return String.join(", ", sourceTable.getClusteringKey().stream().map(Column::getNameWithAscOrDesc).toList());
    }

    private String getStringFromClusteringKey(MSSQLTable table, String delimiter, String alias) {
        return String.join(delimiter, table.getClusteringKey().stream().map(Column::columnName).map(c -> alias + c).toList());
    }

    private String getStringToClusteringKey(MSSQLTable table, String delimiter, String alias) {
        return String.join(delimiter, table.getClusteringKey().stream().map(Column::columnName).map(c -> alias + c).toList());
    }

    private String getStringFromToClusteringKey(MSSQLTable table, String delimiter, String alias) {
        String from = String.join(delimiter, table.getClusteringKey().stream().map(Column::fromName).map(c -> alias + c).toList());
        String to = String.join(delimiter, table.getClusteringKey().stream().map(Column::toName).map(c -> alias + c).toList());
        return from + delimiter + to;
    }

    private String getStringFromToClusteringKeyWithType(MSSQLTable table, String delimiter) {
        String from = String.join(delimiter, table.getClusteringKey().stream().map(Column::fromNameWithType).toList());
        String to = String.join(delimiter, table.getClusteringKey().stream().map(Column::toNameWithType).toList());
        return from + delimiter + to;
    }

    @Override
    public void createChunkTable() throws SQLException {
        if (getOutboxTable() == null) setOutboxTable(new PseudoTable("dbo", "_chunk"));
        Connection connection = this.getPoolConnection();
        createSequence(connection, false);
        Statement createTable = connection.createStatement();
        createTable.executeUpdate(DDL_CREATE_CHUNK_TABLE
                .replace("$schemaName", schemaName())
                .replace("$tableName", getOutboxTable().getTableName()));
        createTable.close();
        connection.commit();
        connection.close();
    }

    private String schemaName() {
        return getOutboxTable().getSchemaName() == null ? "dbo" : getOutboxTable().getSchemaName();
    }

    @Override
    public void dropChunkTable(List<Config> configs) throws SQLException {
        Connection connection = this.getPoolConnection();
        for (Config config : configs) {
            Statement dropTable = connection.createStatement();
            dropTable.executeUpdate(DDL_DROP_CHUNK_TABLE
                    .replace("$schemaName", schemaName())
                    .replace("$tableName", "_ext_" + config.fromTableName()));
            dropTable.close();
        }
        Statement dropTable = connection.createStatement();
        dropTable.executeUpdate(DDL_DROP_CHUNK_TABLE
                .replace("$schemaName", schemaName())
                .replace("$tableName", getOutboxTable().getTableName()));
        dropTable.close();
        connection.commit();
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
    public List<Chunk<?, ?, ?, ?>> getChunkList(List<Config> configs, Storage targetStorage) throws SQLException {
        List<Chunk<?, ?, ?, ?>> chunks = new ArrayList<>();
        for (Config config : configs) {
            Table sourceTable = this.configToTable(config.fromSchemaName(), config.fromTableName());
            Table targetTable = targetStorage.configToTable(config.toSchemaName(), config.toTableName());
            this.enrichTable(sourceTable);
            targetStorage.enrichTable(sourceTable, targetTable);
//            targetTable.getPkColumns().forEach(c -> log.info("PK: {} {} {}", targetTable.getTableName(), c.columnName(), c.columnPosition(), c.ascOrDesc()));
            List<Column2Column> c2c = getColumn2Column(sourceTable, targetTable, config);
            Table2Table t2t = getTable2Table(sourceTable, targetTable, c2c, config);
            String sql = buildStartEndOfChunk(config, sourceTable);
            log.debug("Query of chunks for table {}.{}: {}", t2t.sourceTable().getSchemaName(), t2t.sourceTable().getTableName(), sql);
            String fetchQuery = buildFetchStatement(config, t2t);
            String alias = config.fromTableAlias();
            String addFetchQuery = " AND " + buildConditionBlock(((MSSQLTable)t2t.sourceTable()).getClusteringKey(), false, alias);
            String orderByClause = targetTable.buildOrderBy(config);
            log.info("Fetch query: {} {} {}", fetchQuery, addFetchQuery, orderByClause);
            Connection sourceSession = this.getPoolConnection();
            PreparedStatement preparedStatement = sourceSession.prepareStatement(sql);
            preparedStatement.setString(1, config.fromSchemaName());
            preparedStatement.setString(2, config.fromTableName());
            preparedStatement.setString(3, config.fromTaskName());
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                String status = rs.getString("status");
                Integer chunkId = rs.getInt("chunk_id");
                Map.Entry<List<Object>, List<Object>> entry = getValues(sourceSession, t2t, chunkId);
//                System.out.println(entry);
                Chunk<?, ?, ?, ?> chunk = new MSSQLChunk<>(
                        chunkId,
                        entry.getKey(),
                        entry.getValue(),
                        config,
                        t2t,
                        ChunkStatus.valueOf(status),
                        fetchQuery,
                        this,
                        targetStorage,
                        orderByClause);
                ((MSSQLChunk<?, ?, ?, ?>)chunk).setAddFetchPredicate(addFetchQuery);
                chunks.add(chunk);
            }
            rs.close();
            preparedStatement.close();
            sourceSession.close();
        }
        return chunks;
    }

    private Map.Entry<List<Object>, List<Object>> getValues(Connection connection, Table2Table t2t, Integer chunkId) throws SQLException {
        MSSQLTable sourceTable = (MSSQLTable) t2t.sourceTable();
        String columnList = getStringFromToClusteringKey(sourceTable, ", ", "");
        String sql = SQL_VALUES_FROM_EXT_TABLE
                .replace("$schemaName", schemaName())
                .replace("$extTableName", sourceTable.getTableName())
                .replace("$columns", columnList);
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setInt(1, chunkId);
        ResultSet rs = ps.executeQuery();
        List<Object> fromValues = new ArrayList<>();
        List<Object> toValues = new ArrayList<>();
        if (rs.next()) {
            for (Column column : sourceTable.getClusteringKey()) {
                Object fromValue = rs.getObject(column.fromName());
                Object toValue = rs.getObject(column.toName());
                fromValues.add(fromValue);
                toValues.add(toValue);
            }
        }
        rs.close();
        ps.close();
        return new AbstractMap.SimpleEntry<>(fromValues, toValues);
    }

    @Override
    public String buildStartEndOfChunk(Config config, Table sourceTable) {
        return "select top(1000) c.chunk_id, c.uuid, c.start_page, c.end_page, c.task_name, c.status from " +
                schemaName() + ".[" + getOutboxTable().getTableName() + "] c, " + schemaName() + ".[_ext_" + sourceTable.getTableName() +
                "] e where c.chunk_id = e.chunk_id and " +
                "c.schema_name = ? and c.table_name = ? and c.task_name = ? " +
                " and c.status in ('ASSIGNED', 'UNASSIGNED', 'PROCESSED_WITH_ERROR') "
                + " order by c.chunk_id ";
    }

    @Override
    public <K, T, S extends AutoCloseable, R> LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        return null;
    }

    @Override
    public String buildFetchStatement(Config config, Table2Table t2t) {
        List<Column2Column> sortedColumn2Columns = t2t.getSortedColumn2ColumnByTargetColumnPosition();
        List<String> asColumns = new ArrayList<>(sortedColumn2Columns
                .stream()
                .filter(c2c -> c2c.sourceColumn() != null)
                .map(c2c -> c2c.sourceExpression() == null ? c2c.sourceColumn().columnName() : c2c.sourceExpression())
                .toList());
/*
        List<String> asColumns = t2t.column2Columns()
                .stream()
                .filter(c2c -> c2c.sourceColumn() != null)
                .map(c2c -> c2c.sourceExpression() == null ? c2c.sourceColumn().columnName() : c2c.sourceExpression())
                .toList();
*/
//        Set<String> set = new HashSet<>();
//        Set<String> set = new HashSet<>(asColumns);
//        List<String> finalList = set.stream().toList();
        String columnToColumn = String.join(", ", asColumns);
//        String alias = (config.fromTableAlias() == null ? "" : config.fromTableAlias());
        String alias = config.fromTableAlias();
        return PGKeywords.SELECT + " " +
                columnToColumn + " " +
                (t2t.ttlColumn() == null ? "" : ( ", " + t2t.ttlColumn().defaultValue() + " as " + t2t.ttlColumn().columnName() + " ")) +
//                (t2t.timestampColumn() == null ? "" : ( ", " + t2t.timestampColumn().defaultValue() + " as " + t2t.timestampColumn().columnName() + " ")) +
                PGKeywords.FROM + " " +
                config.fromSchemaName() +
                "." +
                config.fromTableName() + " " +
                (config.fromTableAlias() == null ? "" : config.fromTableAlias()) + " " +
                (config.fromTableAdds() == null ? "" : config.fromTableAdds()) + " " +
                PGKeywords.WHERE + " " +
                (config.fetchWhereClause() == null ? "" : " ( " + config.fetchWhereClause() + " ) and ") + " " +
                buildConditionBlock(((MSSQLTable)t2t.sourceTable()).getClusteringKey(), true, alias);
//                getStringFromClusteringKey((MSSQLTable<S>) t2t.sourceTable(), " >= ? and ", alias) + " >= ? ";
//                getStringToClusteringKey((MSSQLTable<S>) t2t.sourceTable(), " < ? and ", alias) + " < ? ";
    }

    public String buildConditionBlock(List<Column> columns, boolean isStart, String alias) {
        if (columns == null || columns.isEmpty()) return "";

        String prefix = (alias != null && !alias.isEmpty()) ? alias + "." : "";

        StringBuilder sb = new StringBuilder();
        int size = columns.size();

        for (int i = 0; i < size; i++) {
            if (i > 0) sb.append(" OR ");

            sb.append("(");

            for (int j = 0; j < i; j++) {
                sb.append(prefix).append(columns.get(j).columnName()).append(" = ? AND ");
            }

            Column current = columns.get(i);
            boolean isLast = (i == size - 1);
            String operator;

            if (current.getAscOrDesc().equals("ASC")) {
                operator = isStart ? (isLast ? ">=" : ">") : "<";
            } else {
                operator = isStart ? (isLast ? "<=" : "<") : ">";
            }

            sb.append(prefix).append(current.columnName()).append(" ").append(operator).append(" ?)");
        }

        return "(" + sb.toString() + ")";
    }

    @Override
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?, ?, ?> chunk) {
        return Map.of();
    }

    @Override
    public Table configToTable(String schemaName, String tableName) {
        return new MSSQLTable(schemaName, tableName, null);
    }

    @Override
    public void enrichTable(Table sourceTable) throws SQLException {
        Connection session = getPoolConnection();
        sourceTable.enrichTable(session);
        session.close();
    }

    @Override
    public void enrichTable(Table sourceTable, Table targetTable) throws SQLException {
        Connection session = getPoolConnection();
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
        if ((config.columnToColumn() != null && !config.columnToColumn().isEmpty()) && config.avroSchema() == null) {
            for (Map.Entry<String,String> entry : config.columnToColumn().entrySet()) {
                Column sourceColumn = sourceTable.getColumns().stream()
//                        .peek(c -> log.info("{} {}", entry.getKey(), c.columnName()))
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
        if ((config.expressionToColumn() != null && !config.expressionToColumn().isEmpty()) && config.avroSchema() == null) {
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
        if ((config.columnToColumn() != null && !config.columnToColumn().isEmpty()) && config.avroSchema() != null) {
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
        if ((config.expressionToColumn() != null && !config.expressionToColumn().isEmpty()) && config.avroSchema() != null) {
            for (Map.Entry<String, String> entry : config.expressionToColumn().entrySet()) {
                Column targetColumn = columnFromAvro(config.avroSchema(), entry.getValue(), avroFieldPosition++);
                column2Column.add(new Column2Column(targetColumn, targetColumn, entry.getKey()));
            }
        }
        return column2Column;
    }

    @Override
    public Table2Table getTable2Table(Table sourceTable, Table targetTable, List<Column2Column> c2c, Config config) {
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
}
