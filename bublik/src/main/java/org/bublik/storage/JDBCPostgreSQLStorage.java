package org.bublik.storage;

import org.bublik.exception.TableNotExistsException;
import org.bublik.model.*;
import org.bublik.service.JDBCStorageService;
import org.bublik.service.TableService;
import org.bublik.task.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.bublik.constants.SQLConstants.*;

public class JDBCPostgreSQLStorage extends JDBCStorage implements JDBCStorageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCPostgreSQLStorage.class);

    public JDBCPostgreSQLStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
    }

    @Override
    public void initiateTargetThread(List<Future<LogMessage>> futures, List<Config> configs, ExecutorService executorService) throws SQLException {
        Connection connection = this.getConnection();
        if (getConnectionProperty().getInitPGChunks()) {
            fillCtidChunks(connection, configs);
        }
        Map<Integer, Chunk<?>> chunkMap = new TreeMap<>(getChunkMap(connection,configs));
        for (Map.Entry<Integer, Chunk<?>> i : chunkMap.entrySet()) {
            Table table = TableService.getTable(connection, i.getValue().getConfig().fromSchemaName(), i.getValue().getConfig().fromTableName());
            if (table.exists(connection)) {
                Map<String, Integer> orderedColumns = new HashMap<>();
                i.getValue().getConfig().columnToColumn().forEach((k, v) -> orderedColumns.put(k, null));
                futures.add(executorService.submit(new Worker(i.getValue(), orderedColumns)));
            } else {
                LOGGER.error("\u001B[31mThe Source Table: {}.{} does not exist.\u001B[0m", i.getValue().getSourceTable().getSchemaName(),
                        i.getValue().getSourceTable().getTableName());
                throw new TableNotExistsException("The Source Table "
                        + i.getValue().getSourceTable().getSchemaName() + "."
                        + i.getValue().getSourceTable().getTableName() + " does not exist.");
            }
        }
        connection.close();
    }

    private Map<Integer, Chunk<Long>> getChunkMap(Connection connection, List<Config> configs) throws SQLException {
        Map<Integer, Chunk<Long>> chunkHashMap = new TreeMap<>();
        String sql = buildStartEndPageOfPGChunk(configs);
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        if (!resultSet.isBeforeFirst()) {
            System.out.println("No chunk definition found in CTID_CHUNKS for : " +
                    configs.stream().map(Config::fromTaskName).collect(Collectors.joining(", ")));
        } else while (resultSet.next()) {
            Config config = findByTaskName(configs, resultSet.getString("task_name"));
            assert config != null;
            chunkHashMap.put(resultSet.getInt("rownum"),
                    new PGChunk<>(
                            resultSet.getInt("chunk_id"),
                            resultSet.getLong("start_page"),
                            resultSet.getLong("end_page"),
                            config,
                            TableService.getTable(connection, config.fromSchemaName(), config.fromTableName()),
                            null,
                            this
                    )
            );
        }
        resultSet.close();
        statement.close();
        return chunkHashMap;
    }

    private String buildStartEndPageOfPGChunk(List<Config> configs) {
        List<String> taskNames = new ArrayList<>();
        configs.forEach(sqlStatement -> taskNames.add(sqlStatement.fromTaskName()));
        return "select row_number() over (order by chunk_id) as rownum, chunk_id, start_page, end_page, task_name from public.ctid_chunks where task_name in ('" +
                String.join("', '", taskNames) + "') " +
                "and status <> 'PROCESSED' ";
    }

    private void fillCtidChunks(Connection connection, List<Config> configs) throws SQLException {
        createTableCtidChunks(connection);
        for (Config config : configs) {
            long reltuples = 0;
            long relpages = 0;
            long max_end_page = 0;
            long heap_blks_total = 0;
            PreparedStatement preparedStatement = connection.prepareStatement(SQL_NUMBER_OF_TUPLES);
            Table table = TableService.getTable(connection, config.fromSchemaName(), config.fromTableName());
            preparedStatement.setString(1, table.getSchemaName().toLowerCase());
            preparedStatement.setString(2, table.getFinalTableName(false));
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next()) {
                reltuples = resultSet.getLong("reltuples");
                relpages = resultSet.getLong("relpages");
            }
            resultSet.close();
            preparedStatement.close();
            preparedStatement = connection.prepareStatement(SQL_NUMBER_OF_RAW_TUPLES);
            preparedStatement.setString(1, table.getSchemaName().toLowerCase() + "." +
                    table.getTableName());
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()) {
                heap_blks_total = resultSet.getLong("heap_blks_total");
            }
            resultSet.close();
            preparedStatement.close();
            double rowsInChunk = reltuples >= 500000 ? 100000d : 10000d;
//            double rowsInChunk = 10000d;
            long v = reltuples <= 0 && relpages <= 1 ? relpages + 1 :
                    (int) Math.round(relpages / (reltuples / rowsInChunk));
            long pagesInChunk = Math.min(v, relpages + 1);
            LOGGER.info("{}.{} \t\t\t relpages : {}\t heap_blks_total : {}\t reltuples : {}\t rowsInChunk : {}\t pagesInChunk : {} ",
                    config.fromSchemaName(),
                    config.fromTableName(),
                    relpages,
                    heap_blks_total,
                    reltuples,
                    rowsInChunk,
                    pagesInChunk);
            PreparedStatement chunkInsert = connection.prepareStatement(DML_BATCH_INSERT_CTID_CHUNKS);
            chunkInsert.setLong(1, pagesInChunk);
            chunkInsert.setString(2, config.fromTaskName());
            chunkInsert.setLong(3, relpages);
            chunkInsert.setLong(4, pagesInChunk);
            int rows = chunkInsert.executeUpdate();
            chunkInsert.close();
            preparedStatement = connection.prepareStatement(SQL_MAX_END_PAGE);
            preparedStatement.setString(1, config.fromTaskName());
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()) {
                max_end_page = resultSet.getLong("max_end_page");
            }
            resultSet.close();
            preparedStatement.close();
            if (heap_blks_total > max_end_page) {
                chunkInsert = connection.prepareStatement(DML_INSERT_CTID_CHUNKS);
                chunkInsert.setLong(1, max_end_page);
                chunkInsert.setLong(2, heap_blks_total);
                chunkInsert.setString(3, config.fromTaskName());
                rows = chunkInsert.executeUpdate();
                chunkInsert.close();
            }
        }
        connection.commit();
    }

    private void createTableCtidChunks(Connection connection) throws SQLException {
        Statement createTable = connection.createStatement();
        createTable.executeUpdate(DDL_CREATE_POSTGRESQL_TABLE_CHUNKS);
        createTable.close();
    }
}
