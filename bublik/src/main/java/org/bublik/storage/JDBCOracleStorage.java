package org.bublik.storage;

import org.bublik.exception.TableNotExistsException;
import org.bublik.model.*;
import org.bublik.service.JDBCStorageService;
import org.bublik.service.StorageService;
import org.bublik.service.TableService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JDBCOracleStorage extends JDBCStorage implements JDBCStorageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCOracleStorage.class);

    public JDBCOracleStorage(StorageClass storageClass, ConnectionProperty connectionProperty) throws SQLException {
        super(storageClass, connectionProperty);
    }

    @Override
    public void startWorker(List<Config> configs) throws SQLException {
        if (hook(configs)) {
            Map<Integer, Chunk<?>> chunkMap = new TreeMap<>(getChunkMap(configs));
            for (Map.Entry<Integer, Chunk<?>> i : chunkMap.entrySet()) {
                Table table = TableService.getTable(connection, i.getValue().getConfig().fromSchemaName(), i.getValue().getConfig().fromTableName());
                if (!table.exists(connection)) {
                    connection.close();
                    LOGGER.error("\u001B[31mThe Source Table: {}.{} does not exist.\u001B[0m", i.getValue().getSourceTable().getSchemaName(),
                            i.getValue().getSourceTable().getTableName());
                    throw new TableNotExistsException("The Source Table "
                            + i.getValue().getSourceTable().getSchemaName() + "."
                            + i.getValue().getSourceTable().getTableName() + " does not exist.");
                }
            }
            connection.close();
            ExecutorService service = Executors.newFixedThreadPool(threadCount);
            chunkMap.forEach((k, v) ->
                    CompletableFuture
                            .supplyAsync(() -> callWorker(v), service)
                            .thenAccept(LogMessage::loggerInfo));
            service.shutdown();
            service.close();
        }
    }

    @Override
    public boolean hook(List<Config> configs) {
        return getConnectionProperty().getCopyPGChunks() == null || getConnectionProperty().getCopyPGChunks();
    }

    @Override
    public LogMessage transferToTarget(ResultSet resultSet) throws SQLException {
        return null;
    }

    public Map<Integer, Chunk<RowId>> getChunkMap(List<Config> configs) throws SQLException {
        Map<Integer, Chunk<RowId>> chunkHashMap = new TreeMap<>();
        String sql = buildStartEndOfChunk(configs);
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.isBeforeFirst()) {
            Storage targetStorage = StorageService.getStorage(getConnectionProperty().getToProperty(), getConnectionProperty());
            StorageService.set(targetStorage);
            while (resultSet.next()) {
                Config config = findByTaskName(configs, resultSet.getString("task_name"));
                assert config != null;
                chunkHashMap.put(resultSet.getInt("rownum"),
                        new OraChunk<>(
                                resultSet.getInt("chunk_id"),
                                resultSet.getRowId("start_rowid"),
                                resultSet.getRowId("end_rowid"),
                                config,
                                TableService.getTable(connection, config.fromSchemaName(), config.fromTableName()),
                                null,
                                this,
                                targetStorage
                        )
                );
            }
        }
        resultSet.close();
        statement.close();
        return chunkHashMap;
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
}
