package org.bublik.storage;

import de.bytefish.pgbulkinsert.exceptions.BinaryWriteFailedException;
import org.bublik.constants.PGKeywords;
import org.bublik.constants.YDBKeywords;
import org.bublik.exception.SourceSQLException;
import org.bublik.exception.TableNotExistsException;
import org.bublik.exception.TargetSQLException;
import org.bublik.model.*;
import org.bublik.service.JDBCStorageService;
import org.bublik.service.TableService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.bublik.exception.Utils.getStackTrace;

public class JDBCYDBStorage extends JDBCStorage implements JDBCStorageService {
    private static final Logger log = LoggerFactory.getLogger(JDBCPostgreSQLStorage.class);
    private static JDBCYDBStorage toInstance;
    private static JDBCYDBStorage fromInstance;

    protected JDBCYDBStorage(StorageClass storageClass, ConnectionProperty connectionProperty, Boolean isSource) throws SQLException {
        super(storageClass, connectionProperty, isSource);
    }

    public static synchronized JDBCYDBStorage getInstance(StorageClass storageClass,
                                                                 ConnectionProperty connectionProperty,
                                                                 Boolean isSource) throws SQLException{
        try {
            if (isSource) {
                if (toInstance == null) {
                    toInstance = new JDBCYDBStorage(storageClass, connectionProperty, isSource);
                }
                return toInstance;
            }
            if (fromInstance == null) {
                fromInstance = new JDBCYDBStorage(storageClass, connectionProperty, isSource);
            }
            return fromInstance;
        } catch (Exception e) {
            log.error("Connection error: {}", getStackTrace(e));
            throw e;
        }
    }

    @Override
    public String buildStartEndOfChunk(List<Config> configs) {
        return "";
    }

    @Override
    public String buildFetchStatement(Config config) {
        return "";
    }

    @Override
    public void sync() throws SQLException {

    }

    @Override
    public Map<Integer, Chunk<?>> getChunkMap(List<Config> configs) throws SQLException {
        return Map.of();
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
                                    Chunk<?> chunk) throws SQLException, BinaryWriteFailedException, SourceSQLException{
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
//            String targetColName = entry.getValue().getColumnName();
            String targetColType = entry.getValue().getColumnType();
//            Integer targetColPosition = entry.getValue().getColumnPosition();
            index++;
//            System.out.println(targetColName + " " + index);
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
                    if (text != null) {
                        ps.setString(index, text);
                    } else {
                        ps.setObject(index, null);
                    }
                    break;
                }
                case "Date": {
                    Date date = rs.getDate(sourceColName);
                    if (date != null) {
                        ps.setDate(index, date);
                    } else {
                        ps.setObject(index, null);
                    }
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
                    if (timestamp != null) {
                        ps.setTimestamp(index, timestamp);
                    } else {
                        ps.setObject(index, null);
                    }
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
                                            null, null, null, null, null)));
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
                                            null, null, null, null, null)));
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
                                            null, null, null, null, null)));
                }
            }
            resultSet.close();
        } catch (SQLException e) {
            log.error("{}", e.getMessage());
        }
        return columnMap;
    }

    @Override
    public void createPrimaryKey(Map<Table, Table> tables, Storage targetStorage) {
    }

    @Override
    public void createIndex(Map<Table, Table> tables, Storage targetStorage) {

    }

    @Override
    public Map<Table, Table> getMapOfTables(List<Config> configs, Storage targetStorage) {
        Map<Table, Table> tables = new HashMap<>();
        for (Config c : configs) {
            tables.put(new YDBTable(c.fromSchemaName(), c.fromTableName()), targetStorage.createTable(c));
        }
        return tables;
    }

    @Override
    public Map<Table, Table> enrichMapOfTables(Map<Table, Table> tables, Storage targetStorage) {
        return Map.of();
    }

    @Override
    public Table createTable(Config config) {
        return new YDBTable(config.toSchemaName(), config.toTableName());
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
