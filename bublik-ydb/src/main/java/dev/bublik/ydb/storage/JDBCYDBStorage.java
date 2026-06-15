package dev.bublik.ydb.storage;

import dev.bublik.core.constants.PGKeywords;
import dev.bublik.core.exception.SourceSQLException;
import dev.bublik.core.model.*;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import dev.bublik.ydb.model.YDBTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static dev.bublik.core.util.Utils.getStackTrace;
import static dev.bublik.ydb.constants.SQLConstants.*;

public class JDBCYDBStorage<K, T, S extends Connection, R> extends JDBCStorage<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(JDBCYDBStorage.class);

    public JDBCYDBStorage(StorageClass storageClass, ConnectionProperty connectionProperty) throws SQLException {
        super(storageClass, connectionProperty);
    }

    @Override
    public String buildStartEndOfChunk(Config config, String chunkTable, Table<S> sourceTable) {
        return "";
    }

    @Override
    public String buildFetchStatement(Config config, Table2Table<S> t2t) {
        return "";
    }

/*
    @Override
    public String buildFetchStatement(Config config) {
        return "";
    }
*/

    @Override
    public Map.Entry<String,Long> getSystemChangeNumberWithTrxId() throws SQLException {
        return null;
    }

    @Override
    public void fulfillChunks(List<Config> configs, boolean synz, int rows, String tableName) throws SQLException {

    }

    @Override
    public void dropChunkTable(List<Config> configs, boolean sync, String tableName) throws SQLException {

    }

    @Override
    public void createGlobalOutbox(String tableName) throws SQLException {
        String[] t = tableName.split("\\.");
        String tName;
        if (t.length == 1) {
            tName = t[0];
        } else {
            tName = t[1];
        }
        Connection connection = getPoolConnection();
        try {
            Statement createTable = connection.createStatement();
            createTable.executeUpdate(DDL_CREATE_OUTBOX_TABLE.replace("$tableName", tName));
            createTable.close();
//            Table table = TableService.getTable(connection, "", "bublik_outbox");
/*
            Table table = configToTable("", "bublik_outbox");
            if (table.exists(connection)) {
                Statement createTable = connection.createStatement();
                createTable.executeUpdate(DDL_DROP_YDB_TABLE_BUBLIK_OUTBOX);
                createTable.close();
            }
            Statement truncateTable = connection.createStatement();
            truncateTable.executeUpdate(DDL_CREATE_OUTBOX_TABLE);
            truncateTable.close();
            connection.commit();
*/
            log.info("Outbox table created successfully");
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
        connection.close();
    }

    @Override
    public List<Chunk<K, T, S, R>> getChunkList(List<Config> configs, String chunkTable, Storage<K, T, S, R> targetStorage) throws SQLException {
        return List.of();
    }

    @Override
    public LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        ResultSet fetchResultSet = (ResultSet) chunk.getResultSet();
        Connection connectionFrom = chunk.getSourceSession();
        if (fetchResultSet.next()) {
            Connection connectionTo = chunk.getTargetSession();
//            Table table = configToTable(chunk.getConfig().toSchemaName(), chunk.getConfig().toTableName());
//            if (table.exists(connectionTo)) {
//                chunk.setTargetTable(table);
                try {
                    LogMessage logMessage = fetchAndCopy(fetchResultSet, chunk, tableName);
                    connectionTo.close();
                    return logMessage;
                } catch (SQLException e) {
                    log.error("ChunkId: {} {}", chunk.getId(), getStackTrace(e));
                    connectionTo.rollback();
                    connectionTo.close();
                    throw e;
                } catch (SourceSQLException s) {
                    connectionFrom.close();
                    log.error("{}", getStackTrace(s));
                    throw s;
                } finally {
                    ;
                }
/*
            } else {
                log.error("The Target Table: {}.{} does not exist.", chunk.getConfig().toSchemaName(),
                        chunk.getConfig().toTableName());
                throw new TableNotExistsException("The Target Table "
                        + chunk.getConfig().toSchemaName() + "/"
                        + chunk.getConfig().toTableName() + " does not exist.");
            }
*/
        } else {
            return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), "NO ROWS FETCH");
        }
    }

    private LogMessage fetchAndCopy(ResultSet fetchResultSet,
                                    Chunk<?, ?, ?, ?> chunk,
                                    String tableName) throws SQLException, SourceSQLException {
        int recordCount = 0;
        Connection connectionTo = (Connection) chunk.getTargetSession();

        try {
//            insertProcessedChunkInfo(connectionTo, (int) chunk.getId(), recordCount, chunk.getConfig().fromTaskName(), tableName);
            insertProcessedChunkInfo(chunk, tableName);
            connectionTo.rollback();
        } catch (SQLException e) {
//            log.error("Error insert into BUBLIK_OUTBOX for chunk {}, start {}, end {}, rows {}, task {}: {}",
//                    chunk.getId(), chunk.getStart(), chunk.getEnd(), chunk.getRows(), chunk.getConfig().fromTaskName(), e.getMessage());
            if (e.getMessage().contains("#2012 Conflict with existing key")) {
                return new LogMessage(
                        chunk.getStartTime(),
                        System.currentTimeMillis(),
                        "The chunk has already been copied");
            } else {
                throw new SQLException(e);
            }
        }

        Map<String, Column> neededColumnsToDB = readTargetColumnsAndTypes(connectionTo, chunk);

        try {
            PreparedStatement ps = connectionTo.prepareStatement(batchInsertStatement(chunk.getConfig(), neededColumnsToDB));
            do {
                prepareBatchInsert(fetchResultSet, ps, neededColumnsToDB);
                recordCount++;
            } while (hasNext(fetchResultSet, chunk));
            ps.executeBatch();
            ps.close();
        } catch (SQLException e) {
            log.error("ON BATCH EXECUTE chunkId = {} {}", chunk.getId(), getStackTrace(e));
            throw e;
        }

        try {
//            insertProcessedChunkInfo(connectionTo, (int) chunk.getId(), recordCount, chunk.getConfig().fromTaskName(), tableName);
            insertProcessedChunkInfo(chunk, tableName);
            connectionTo.commit();
        } catch (SQLException e) {
            log.error("ON COMMIT chunkId = {} {}", chunk.getId(), getStackTrace(e));
            throw e;
        }

/*
        try (PreparedStatement ps = connectionTo.prepareStatement(
                batchInsertStatement(chunk.getConfig(), neededColumnsToDB))
        ) {
            do {
                prepareBatchInsert(fetchResultSet, ps, neededColumnsToDB);
                recordCount++;
            } while (hasNext(fetchResultSet));
            ps.executeBatch();
            chunk.insertProcessedChunkInfo(connectionTo, recordCount);
            connectionTo.commit();
        }
*/

        chunk.setCopied(recordCount);
        return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), "YDB Batch Insert ");
    }

    private void prepareBatchInsert(ResultSet rs,
                                    PreparedStatement ps,
                                    Map<String, Column> neededColumnsToDB) throws SQLException {
        int index = 0;
        for (Map.Entry<String, Column> entry : neededColumnsToDB.entrySet()) {
            String sourceColName = entry.getKey().replaceAll("\"", "");
            String targetColType = entry.getValue().columnType();
            index++;
            switch (targetColType) {
                case "Uint8", "Int8" : {
                    ps.setShort(index, rs.getShort(sourceColName));
                    break;
                }
                case "Uint32", "Int32", "Uint16", "Int16" : {
                    ps.setInt(index, rs.getInt(sourceColName));
                    break;
                }
                case "Uint64", "Int64" : {
                    ps.setLong(index, rs.getLong(sourceColName));
                    break;
                }
                case "Bytes": {
                    byte[] bytes = rs.getBytes(sourceColName);
                    ps.setBytes(index, bytes);
                    break;
                }
                case "Uuid": {
                    String uuid = rs.getString(sourceColName);
                    if (uuid != null) {
                        ps.setObject(index, java.util.UUID.fromString(uuid));
                    } else {
                        ps.setObject(index, null);
                    }
                    break;
                }
                case "Bool": {
                    ps.setBoolean(index, rs.getBoolean(sourceColName));
                    break;
                }
                case "Text": {
                    String text = rs.getString(sourceColName);
                    ps.setString(index, text);
                    break;
                }
                case "Date": {
                    Date date = rs.getDate(sourceColName);
                    ps.setDate(index, date);
                    break;
                }
                case "Float": {
                    ps.setFloat(index, rs.getFloat(sourceColName));
                    break;
                }
                case "Double": {
                    ps.setDouble(index, rs.getDouble(sourceColName));
                    break;
                }
                case "Timestamp": {
                    Timestamp timestamp = rs.getTimestamp(sourceColName);
                    ps.setTimestamp(index, timestamp);
                    break;
                }
            }
        }
        ps.addBatch();
    }

    private boolean hasNext(ResultSet resultSet, Chunk<?, ?, ?, ?> chunk) throws SQLException {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            log.info("ChunkId: {} {}", chunk.getId(), getStackTrace(e));
            throw e;
        }
    }

    @Override
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?, ?, ?> chunk) {
        Map<String, Column> columnMap = new HashMap<>();
        try {
            ResultSet resultSet = connectionTo.getMetaData().getColumns(
                    null,
                    chunk.getT2t().targetTable().getSchemaName().toLowerCase(),
                    chunk.getT2t().targetTable().getFinalTableName(false),
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
                            .forEach(i -> columnMap.put(i.getKey(),
                                    new Column(
                                            columnPosition,
                                            i.getValue(),
                                            columnType.equals("bigserial") ? "bigint" : columnType,
                                            null, null, null, null, null, 0 , null, 0, null, false, false, false)));
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
                                            null, null, null, null, null, 0 , null, 0, null, false, false, false)));
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
                                            null, null, null, null, null, 0 , null, 0, null, false, false, false)));
                }
            }
            resultSet.close();
        } catch (SQLException e) {
            log.error("{}", e.getMessage());
        }
        return columnMap;
    }

    @Override
    public Table configToTable(String schemaName, String tableName) {
        return new YDBTable(schemaName, tableName);
    }

    @Override
    public void enrichTable(Table<S> sourceTable, Table<S> targetTable) throws SQLException {
        S session = getPoolConnection();
        targetTable.enrichTable(session);
        session.close();
    }

    @Override
    public List<Column2Column> getColumn2Column(Table<S> sourceTable, Table<S> targetTable, Config config) {
        return List.of();
    }

    @Override
    public Table2Table<S> getTable2Table(Table<S> sourceTable, Table<S> targetTable, List<Column2Column> c2c, Config config) {
        return null;
    }

    @Override
    public void initCache(List<Config> configs) throws SQLException {
        log.info("Cache is not implemented for YDB");
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
    public <W extends Serializable> byte[] intervalYM2Interval(W intervalym) {
        return null;
    }

    @Override
    public <W extends Serializable> byte[] intervalDS2Interval(W intervalds) {
        return null;
    }

    @Override
    public void createUniqueConstraints() {

    }

    public String batchInsertStatement(Config config) {
        List<String> strings = new ArrayList<>();
        Map<String, String> columnToColumnMap = config.columnToColumn();
        Map<String, String> expressionToColumnMap = config.expressionToColumn();
        if (columnToColumnMap != null) {
            strings.addAll(columnToColumnMap.values());
        }
        if (expressionToColumnMap != null) {
            strings.addAll(expressionToColumnMap.values());
        }
        String columnToColumn = String.join(", ", strings);
        String columnToColumnQ = " ?,".repeat(Math.max(0, strings.size() - 1)) +
                " ?"; // last column without comma
        return PGKeywords.INSERT  + " INTO `" +
                config.toTableName() + "` ( " +
                columnToColumn + " ) VALUES ( " +
                columnToColumnQ + " ) ";
    }

    public String batchInsertStatement(Config config, Map<String, Column> neededColumnsToDB) {
        List<String> strings = new ArrayList<>();
        for (Map.Entry<String, Column> entry : neededColumnsToDB.entrySet()) {
            strings.add(entry.getValue().columnName());
        }
        String columnToColumn = String.join(", ", strings);
        String columnToColumnQ = " ?,".repeat(Math.max(0, strings.size() - 1)) +
                " ?"; // last column without comma
        return //YDBKeywords.BULK + " " + YDBKeywords.UPSERT +
                "BULK UPSERT INTO `" +
                config.toTableName() + "` ( " +
                columnToColumn + " ) VALUES ( " +
                columnToColumnQ + " ) ";
    }

    @Override
    public void dropOutboxTable(boolean sync, String tableName) throws SQLException {
        String[] t = tableName.split("\\.");
        String tName;
        if (t.length == 1) {
            tName = t[0];
        } else {
            tName = t[1];
        }
        try {
            Connection connection = getPoolConnection();
            Statement dropTable = connection.createStatement();
            dropTable.executeUpdate(DDL_DROP_OUTBOX_TABLE.replace("$tableName", tName));
            dropTable.close();
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void insertProcessedChunkInfo(Chunk<?, ?, ?, ?> chunk, String tableName) {
        try {
            String[] t = tableName.split("\\.");
            String tName;
            if (t.length == 1) {
                tName = t[0];
            } else {
                tName = t[1];
            }
            Connection connection = (Connection) chunk.getTargetSession();
            PreparedStatement ps = connection.prepareStatement(DML_INSERT_OUTBOX_TABLE.replace("$tableName", tName));
            ps.setInt(1, (int) chunk.getId());
            ps.setString(2, chunk.getConfig().fromTaskName());
            ps.setLong(3, chunk.getCopied());
            long r = ps.executeUpdate();
            ps.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

/*
    @Override
    public void insertProcessedChunkInfo(Connection connection, int chunkId, int rows, String taskName, String tableName) throws SQLException {
        String[] t = tableName.split("\\.");
        String tName;
        if (t.length == 1) {
            tName = t[0];
        } else {
            tName = t[1];
        }
        PreparedStatement chunkInsert = connection.prepareStatement(DML_INSERT_OUTBOX_TABLE.replace("$tableName", tName));
        chunkInsert.setLong(1, chunkId);
        chunkInsert.setString(2, taskName);
        chunkInsert.setLong(3, rows);
        long r = chunkInsert.executeUpdate();
        chunkInsert.close();
    }
*/

    @Override
    public void enrichTable(Table<S> targetTable) throws SQLException {
        targetTable.enrichTable(getSession());
    }
}
