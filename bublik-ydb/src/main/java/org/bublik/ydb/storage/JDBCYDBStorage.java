package org.bublik.ydb.storage;

import org.bublik.core.constants.PGKeywords;
import org.bublik.core.constants.YDBKeywords;
import org.bublik.core.exception.SourceSQLException;
import org.bublik.core.exception.TableNotExistsException;
import org.bublik.core.exception.TargetSQLException;
import org.bublik.core.model.*;
import org.bublik.core.service.JDBCStorageService;
import org.bublik.core.storage.JDBCStorage;
import org.bublik.core.storage.StorageClass;
import org.bublik.ydb.model.YDBTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.bublik.core.constants.SQLConstants.DDL_CREATE_YDB_TABLE_BUBLIK_OUTBOX;
import static org.bublik.core.constants.SQLConstants.DDL_DROP_YDB_TABLE_BUBLIK_OUTBOX;
import static org.bublik.core.util.Utils.getStackTrace;

public class JDBCYDBStorage extends JDBCStorage implements JDBCStorageService {
    private static final Logger log = LoggerFactory.getLogger(JDBCYDBStorage.class);

    public JDBCYDBStorage(StorageClass storageClass, ConnectionProperty connectionProperty) throws SQLException {
        super(storageClass, connectionProperty);
    }

    @Override
    public String buildStartEndOfChunk(List<Config> configs) {
        return "";
    }

    @Override
    public String buildFetchStatement(Config config, Table sourceTable) {
        return buildFetchStatement(config);
    }

    @Override
    public String buildFetchStatement(Config config) {
        return "";
    }

    @Override
    public Map.Entry<String,Long> getSystemChangeNumberWithTrxId() throws SQLException {
        return null;
    }

    @Override
    public void createChunks(Connection connection, List<Config> configs, boolean synz, int rows) throws SQLException {

    }

    @Override
    public void createOutbox() throws SQLException {
        Connection connection = getConnection();
        try {
//            Table table = TableService.getTable(connection, "", "bublik_outbox");
            Table table = configToTable("", "bublik_outbox");
            if (table.exists(connection)) {
                Statement createTable = connection.createStatement();
                createTable.executeUpdate(DDL_DROP_YDB_TABLE_BUBLIK_OUTBOX);
                createTable.close();
            }
            Statement truncateTable = connection.createStatement();
            truncateTable.executeUpdate(DDL_CREATE_YDB_TABLE_BUBLIK_OUTBOX);
            truncateTable.close();
            connection.commit();
            log.info("Outbox table created successfully");
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
        connection.close();
    }

    @Override
    public List<Chunk<?>> getChunkList(List<Config> configs, Connection connection) throws SQLException {
        return List.of();
    }

    @Override
    public LogMessage transferToTarget(Chunk<?> chunk) throws SQLException {
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
            Table table = configToTable(chunk.getConfig().toSchemaName(), chunk.getConfig().toTableName());
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
                } finally {
                    ;
                }
            } else {
                log.error("\u001B[31mThe Target Table: {}.{} does not exist.\u001B[0m", chunk.getConfig().toSchemaName(),
                        chunk.getConfig().toTableName());
                throw new TableNotExistsException("The Target Table "
                        + chunk.getConfig().toSchemaName() + "/"
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
                                    Chunk<?> chunk) throws SQLException, SourceSQLException {
        int recordCount = 0;

//        chunk.insertProcessedChunkInfo(connectionTo, recordCount);
//        connectionTo.rollback();
        try {
            chunk.insertProcessedChunkInfo(connectionTo, recordCount);
            connectionTo.rollback();
        } catch (SQLException e) {
            log.error("chunkId = {} {}", chunk.getId(), getStackTrace(e));
            throw new SQLException(e);
/*
            connectionTo.rollback();
            return new LogMessage(
                    0,
                    chunk.getStartTime(),
                    System.currentTimeMillis(),
                    "The chunk has already been copied",
                    chunk);
*/
        }

        Map<String, Column> neededColumnsToDB = readTargetColumnsAndTypes(connectionTo, chunk);
/*
        neededColumnsToDB.forEach((key, value) ->
                log.info("{} = {}:{}:{}", key, value.getColumnName(),
                        value.getColumnType(), value.getColumnPosition()));
*/

//        log.info("{}", batchInsertStatement(chunk.getConfig(), neededColumnsToDB));

        try (PreparedStatement ps = connectionTo.prepareStatement(
                batchInsertStatement(chunk.getConfig(), neededColumnsToDB)
        )) {
            do {
                prepareBatchInsert(fetchResultSet, ps, neededColumnsToDB);
                recordCount++;
            } while (hasNext(fetchResultSet));
            ps.executeBatch();
            chunk.insertProcessedChunkInfo(connectionTo, recordCount);
            connectionTo.commit();
        }


        return new LogMessage(
                recordCount,
                chunk.getStartTime(),
                System.currentTimeMillis(),
                "YDB Batch Insert ",
                chunk);
    }

    private void prepareBatchInsert(ResultSet rs,
                                    PreparedStatement ps,
                                    Map<String, Column> neededColumnsToDB) throws SQLException {
        int index = 0;
        for (Map.Entry<String, Column> entry : neededColumnsToDB.entrySet()) {
            String sourceColName = entry.getKey().replaceAll("\"", "");
            String targetColType = entry.getValue().getColumnType();
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

    private boolean hasNext(ResultSet resultSet) throws SourceSQLException {
        try {
            return resultSet.next();
        } catch (SQLException e) {
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
                                            null, null, null, null, null, 0 , null, 0, null)));
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
                                            null, null, null, null, null, 0 , null, 0, null)));
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
                                            null, null, null, null, null, 0 , null, 0, null)));
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
    public void createPrimaryKeys() {
    }

    @Override
    public void createIndexes() {

    }

    @Override
    public void createForeignKeys() {

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

    }

    @Override
    public void enrichSourceTables(Connection connection) {

    }

    @Override
    public void createTables() {

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
            strings.add(entry.getValue().getColumnName());
        }
        String columnToColumn = String.join(", ", strings);
        String columnToColumnQ = " ?,".repeat(Math.max(0, strings.size() - 1)) +
                " ?"; // last column without comma
        return YDBKeywords.BULK + " " + YDBKeywords.UPSERT + " INTO `" +
                config.toTableName() + "` ( " +
                columnToColumn + " ) VALUES ( " +
                columnToColumnQ + " ) ";
    }
}
