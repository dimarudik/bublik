package org.bublik.oracle.storage;

import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import org.bublik.core.constants.ChunkStatus;
import org.bublik.core.constants.PGKeywords;
import org.bublik.core.model.*;
import org.bublik.core.storage.JDBCStorage;
import org.bublik.core.storage.StorageClass;
import org.bublik.oracle.model.OraChunk;
import org.bublik.oracle.model.OraTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.bublik.core.util.Utils.getStackTrace;
import static org.bublik.oracle.constants.SQLConstants.*;

public class JDBCOracleStorage<K extends Integer, T extends RowId, S extends Connection, R extends ResultSet> extends JDBCStorage<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(JDBCOracleStorage.class);
//    private static final int HIGH_BIT_FLAG = 0x80000000;

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
    public void createChunks(List<Config> configs, boolean synz, int rows, String tableName) throws SQLException {
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
    public void dropChunkTable(boolean sync, String tableName) throws SQLException {

    }

    @Override
    public void createOutbox(String tableName) throws SQLException {

    }

    @Override
    public void insertProcessedChunkInfo(Connection connection, int chunkId, int rows, String taskName, String tableName) throws SQLException {

    }

    @Override
    public void dropOutboxTable(boolean sync, String tableName) throws SQLException {

    }

    @Override
    public List<Chunk<K, T, S, R>> getChunkList(List<Config> configs, String chunkTable) throws SQLException {
        Connection connection = getConnection();
        List<Chunk<K, T, S, R>> chunkHashMap = new ArrayList<>();
        String sql = buildStartEndOfChunk(configs, chunkTable);
        log.debug("SQL to fetch metadata of chunks: \n{}", sql);
        StringBuffer sb = new StringBuffer();
        for (Config c : configs)
            sb.append("\n").append(buildFetchStatement(c));
        log.debug("SQL to fetch chunks: {}", sb);
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.isBeforeFirst()) {
            while (resultSet.next()) {
                Config config = findByTaskName(configs, resultSet.getString("task_name"));
                Table sourceTable = configToTable(config.fromSchemaName(), config.fromTableName());
                String status = resultSet.getString("status");
                chunkHashMap.add(
                        new OraChunk<>(
                                (K)Integer.valueOf(resultSet.getInt("chunk_id")),
                                (T)resultSet.getRowId("start_rowid"),
                                (T)resultSet.getRowId("end_rowid"),
                                config,
                                sourceTable,
                                ChunkStatus.valueOf(status),
                                null,
                                this
                        )
                );
            }
        }
        resultSet.close();
        statement.close();
        return chunkHashMap;
    }

    @Override
    public String buildStartEndOfChunk(List<Config> configs, String chunkTable) {
        List<String> taskAndWhere = new ArrayList<>();
        configs.forEach(sqlStatement -> {
            String tmp = sqlStatement.fromTaskWhereClause() == null ? "'" : "' and " + sqlStatement.fromTaskWhereClause();
            taskAndWhere.add(sqlStatement.fromTaskName() + tmp);
        });
        String part1 = """
                select rownum, chunk_id, start_rowid, end_rowid, start_id, end_id, task_name, status from (
                \tselect chunk_id, start_rowid, end_rowid, start_id, end_id, task_name, status from (
                """;
        String tmpPart2 = "\t\tselect chunk_id, start_rowid, end_rowid, start_id, end_id, task_name, status from user_parallel_execute_chunks where " +
                "status <> 'PROCESSED' " + " and task_name = '";
        String part2 = tmpPart2 + String.join(" and rownum <= 1000 union all \n" + tmpPart2, taskAndWhere);
        String part3 = "\n\t) order by ora_hash(concat(task_name,start_rowid)) \n) order by 1";
        return  part1 + part2 + part3;
    }

    @Override
    public String buildFetchStatement(Config config, Table sourceTable) {
        return buildFetchStatement(config);
    }

    @Override
    public String buildFetchStatement(Config config) {
        List<String> strings = new ArrayList<>();
        Map<String, String> columnToColumnMap = config.columnToColumn();
        Map<String, String> expressionToColumnMap = config.expressionToColumn();
//        Map<String, EncryptedColumn> encryptedEntityMap = config.expressionToCrypto();
//        Map<String, String> cryptoToColumnMap = config.cryptoToColumn();
        if (columnToColumnMap != null) {
            strings.addAll(columnToColumnMap.keySet());
        }
        if (expressionToColumnMap != null) {
            strings.addAll(expressionToColumnMap.keySet());
        }
/*
        if (encryptedEntityMap != null) {
            strings.addAll(encryptedEntityMap.keySet());
        }
        if (cryptoToColumnMap != null) {
            strings.addAll(cryptoToColumnMap.keySet());
        }
*/
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

    @Override
    public void createTables() {

    }

    @Override
    public Table<S> configToTable(String schemaName, String tableName) {
        return new OraTable<>(schemaName, tableName);
    }

/*
    @Override
    public void setTargetSession(S targetSession) {
        setConnection(targetSession);
    }
*/
}
