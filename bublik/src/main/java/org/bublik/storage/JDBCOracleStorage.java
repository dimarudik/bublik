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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class JDBCOracleStorage extends JDBCStorage implements JDBCStorageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCOracleStorage.class);
    private static JDBCOracleStorage instance;

    private JDBCOracleStorage(StorageClass storageClass,
                              ConnectionProperty connectionProperty,
                              Boolean isSource) throws SQLException {
        super(storageClass, connectionProperty, isSource);
    }

    public static synchronized JDBCOracleStorage getInstance(StorageClass storageClass,
                                                             ConnectionProperty connectionProperty,
                                                             Boolean isSource) throws SQLException{
        if (instance == null) {
            instance = new JDBCOracleStorage(storageClass, connectionProperty, isSource);
        }
        return instance;
    }

/*
    @Override
    public boolean hook(List<Config> configs) {
        return getConnectionProperty().getCopyPGChunks() == null || getConnectionProperty().getCopyPGChunks();
    }
*/

    @Override
    public LogMessage transferToTarget(Chunk<?> chunk) throws SQLException {
        return null;
    }

    @Override
    public Map<Integer, Chunk<?>> getChunkMap(List<Config> configs) throws SQLException {
        Map<Integer, Chunk<?>> chunkHashMap = new TreeMap<>();
        String sql = buildStartEndOfChunk(configs);
        PreparedStatement statement = initialConnection.prepareStatement(sql);
//        System.out.println(sql);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.isBeforeFirst()) {
//            Storage targetStorage = StorageService.getStorage(getConnectionProperty().getToProperty(), getConnectionProperty());
//            StorageService.set(targetStorage);
            while (resultSet.next()) {
                Config config = findByTaskName(configs, resultSet.getString("task_name"));
                Table sourceTable = TableService.getTable(initialConnection, config.fromSchemaName(), config.fromTableName());
                if (!sourceTable.exists(initialConnection)) {
                    initialConnection.close();
                    LOGGER.error("\u001B[31mThe Source Table: {}.{} does not exist.\u001B[0m", sourceTable.getSchemaName(),
                            sourceTable.getTableName());
                    throw new TableNotExistsException(sourceTable.getSchemaName(), sourceTable.getTableName());
                }
                chunkHashMap.put(resultSet.getInt("rownum"),
                        new OraChunk<>(
                                resultSet.getInt("chunk_id"),
                                resultSet.getRowId("start_rowid"),
                                resultSet.getRowId("end_rowid"),
                                config,
                                sourceTable,
                                this//,
//                                targetStorage
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
