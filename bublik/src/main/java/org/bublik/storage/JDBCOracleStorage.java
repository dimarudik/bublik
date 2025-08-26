package org.bublik.storage;

import org.bublik.constants.PGKeywords;
import org.bublik.exception.TableNotExistsException;
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

import static org.bublik.constants.SQLConstants.*;
import static org.bublik.exception.Utils.getStackTrace;

public class JDBCOracleStorage extends JDBCStorage implements JDBCStorageService {
    private static final Logger log = LoggerFactory.getLogger(JDBCOracleStorage.class);

    private static JDBCOracleStorage fromInstance;

    private JDBCOracleStorage(StorageClass storageClass,
                              ConnectionProperty connectionProperty,
                              Boolean isSource) throws SQLException {
        super(storageClass, connectionProperty, isSource);
    }

    public static synchronized JDBCOracleStorage getInstance(StorageClass storageClass,
                                                             ConnectionProperty connectionProperty,
                                                             Boolean isSource) throws SQLException{
        if (fromInstance == null) {
            fromInstance = new JDBCOracleStorage(storageClass, connectionProperty, isSource);
        }
        return fromInstance;
    }

    @Override
    public LogMessage transferToTarget(Chunk<?> chunk) throws SQLException {
        return null;
    }

    @Override
    public Map.Entry<String, Long> getSystemChangeNumberWithTrxId() throws SQLException {
        return null;
    }

    @Override
    public void createChunks(List<Config> configs, boolean synz, int rows) throws SQLException {
        Connection connection = getConnection();
        for (Config config : configs) {
            try {
                CallableStatement dropTask = connection.prepareCall(PLSQL_DROP_TASK);
                dropTask.setString(1, config.fromTaskName());
                dropTask.execute();
                dropTask.close();
            } catch (SQLException e) {
//                log.error("{}", getStackTrace(e));
                log.warn("Task {} does not exist", config.fromTaskName());
            }
        }
        for (Config config : configs) {
            try {
                CallableStatement createTask = connection.prepareCall(PLSQL_CREATE_TASK);
                createTask.setString(1, config.fromTaskName());
                log.info("Creating tasks... {} {}", PLSQL_CREATE_TASK, config.fromTaskName());
                createTask.execute();
                createTask.close();
            } catch (SQLException e) {
                log.error("{}", getStackTrace(e));
                connection.close();
                throw e;
            }
        }

        for (Config config : configs) {
            try {
//                Table table = TableService.getTable(connection, config.fromSchemaName(), config.fromTableName());
                Table table = configToTable(config.fromSchemaName(), config.fromTableName());
                CallableStatement createChunk = connection.prepareCall(PLSQL_CREATE_CHUNK);
                createChunk.setString(1, config.fromTaskName());
                createChunk.setString(2, table.getSchemaName().toUpperCase());
                createChunk.setString(3, table.getFinalTableName(false));
                createChunk.setInt(4, rows);
                createChunk.execute();
                createChunk.close();
            } catch (SQLException e) {
                log.error("{}", getStackTrace(e));
                connection.close();
                throw e;
            }
        }
        log.info("Ctid chunks created successfully");
        connection.close();
    }

    @Override
    public void createOutbox() throws SQLException {

    }

    @Override
    public Map<Integer, Chunk<?>> getChunkMap(List<Config> configs) throws SQLException {
//        Map<Integer, Chunk<?>> chunkHashMap = new TreeMap<>();
        Map<Integer, Chunk<?>> chunkHashMap = new HashMap<>();
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
                Config config = findByTaskName(configs, resultSet.getString("task_name"));
//                Table sourceTable = TableService.getTable(initialConnection, config.fromSchemaName(), config.fromTableName());
                Table sourceTable = configToTable(config.fromSchemaName(), config.fromTableName());
                if (!sourceTable.exists(initialConnection)) {
                    initialConnection.close();
                    log.error("\u001B[31mThe Source Table: {}.{} does not exist.\u001B[0m", sourceTable.getSchemaName(),
                            sourceTable.getTableName());
                    throw new TableNotExistsException(sourceTable.getSchemaName(), sourceTable.getTableName());
                }
                String query = buildFetchStatement(config);
                chunkHashMap.put(resultSet.getInt("rownum"),
                        new OraChunk<>(
                                resultSet.getInt("chunk_id"),
                                resultSet.getRowId("start_rowid"),
                                resultSet.getRowId("end_rowid"),
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
    public Map<Integer, Chunk<?>> getChunkMap(List<Config> configs, Connection connection) throws SQLException {
        return Map.of();
    }

    @Override
    public String buildStartEndOfChunk(List<Config> configs) {
        List<String> taskAndWhere = new ArrayList<>();
        configs.forEach(sqlStatement -> {
            String tmp = sqlStatement.fromTaskWhereClause() == null ? "'" : "' and " + sqlStatement.fromTaskWhereClause();
            taskAndWhere.add(sqlStatement.fromTaskName() + tmp);
        });
        String part1 = """
                select rownum, chunk_id, start_rowid, end_rowid, start_id, end_id, task_name from (
                \tselect chunk_id, start_rowid, end_rowid, start_id, end_id, task_name from (
                """;
        String tmpPart2 = "\t\tselect chunk_id, start_rowid, end_rowid, start_id, end_id, task_name from user_parallel_execute_chunks where " +
                "status <> 'PROCESSED' " + "and task_name = '";
        String part2 = tmpPart2 + String.join(" union all \n" + tmpPart2, taskAndWhere);
        String part3 = "\n\t) order by ora_hash(concat(task_name,start_rowid)) \n) order by 1";
        return  part1 + part2 + part3;
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
        return  PGKeywords.SELECT + " /* bublik */ " +
                (config.fetchHintClause() == null ? "" : config.fetchHintClause()) + " " +
                columnToColumn + " " +
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
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?> chunk) {
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
    public void enrichSourceTables() {
        Map<Table, Table> tables = getTables();
        try {
            Connection sourceConnection = getConnection();
            for (Map.Entry<Table, Table> entry : tables.entrySet()) {
                Table sourceTable = entry.getKey();
                List<Column> allSourceColumns = sourceTable.getAllColumns(sourceConnection);
                sourceTable.setColumns(allSourceColumns);
            }
            sourceConnection.close();
        } catch (SQLException e) {
            log.error("{}", getStackTrace(e));
        }
    }

    @Override
    public void createTables() {

    }

    @Override
    public Table configToTable(String schemaName, String tableName) {
        return new OraTable(schemaName, tableName);
    }
}
