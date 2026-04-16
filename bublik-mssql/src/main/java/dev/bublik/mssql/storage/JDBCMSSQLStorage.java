package dev.bublik.mssql.storage;

import dev.bublik.core.model.*;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import dev.bublik.mssql.model.MSSQLTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static dev.bublik.mssql.constants.SQLConstants.*;

public class JDBCMSSQLStorage<K extends Integer, T extends String, S extends Connection, R extends ResultSet> extends JDBCStorage<K, T, S, R> {
    private static final Logger log = LoggerFactory.getLogger(JDBCMSSQLStorage.class);

    public JDBCMSSQLStorage(StorageClass storageClass, ConnectionProperty connectionProperty) throws SQLException {
        super(storageClass, connectionProperty);
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
    public void fulfillChunks(List<Config> configs, boolean sync, int rows, String tableName) throws SQLException {
        Connection connection = getConnection();
        createSchema(connection, sync);
        createSequence(connection, sync);
        createChunkTable(connection, sync, tableName);
        connection.commit();
        for (Config config : configs) {
            MSSQLTable<S> sourceTable = (MSSQLTable<S>) configToTable(config.fromSchemaName(), config.fromTableName());
            List<Column> clusteringKey = sourceTable.obtainClusteringKey((S)connection);
            sourceTable.setClusteringKey(clusteringKey);
            createChunkExtTable(connection, sync, sourceTable);
            log.info("Fulfilling chunk table {} with data from table {}", tableName, sourceTable.getTableFullName());
        }
        log.info("Chunk table {} created successfully", tableName);
    }

    private void createSequence(Connection connection, boolean sync) throws SQLException {
        Statement createTable = connection.createStatement();
        createTable.executeUpdate(DDL_CREATE_SEQUENCE);
        createTable.close();
        connection.commit();
    }

    private void createSchema(Connection connection, boolean sync) throws SQLException {
        Statement createTable = connection.createStatement();
        createTable.executeUpdate(DDL_CREATE_SCHEMA);
        createTable.close();
        connection.commit();
    }

    private void createChunkExtTable(Connection connection, boolean sync, MSSQLTable<S> table) throws SQLException {
        Statement createTable = connection.createStatement();
        String columnList = ", " + String.join(", ", table.getClusteringKey().stream().map(Column::getColumnNameWithType).toList());
        String sql = DDL_CREATE_CHUNK_EXT_TABLE
                .replace("$tableName", table.getTableName())
                .replace("$columns", columnList);
        log.info("Creating chunk ext table: {}", sql);
        createTable.executeUpdate(sql);
        createTable.close();
        connection.commit();
    }

    private void createChunkTable(Connection connection, boolean sync, String chunkTableName) throws SQLException {
        Statement createTable = connection.createStatement();
        createTable.executeUpdate(DDL_CREATE_CHUNK_TABLE.replace("$tableName", chunkTableName));
        createTable.close();
        connection.commit();
    }

    @Override
    public void dropChunkTable(List<Config> configs, boolean sync, String tableName) throws SQLException {
/*
        Connection connection = getConnection();
        try (Statement dropTable = connection.createStatement()) {
            dropTable.executeUpdate(DDL_DROP_CHUNK_TABLE.replace("$tableName", tableName));
            dropTable.close();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            log.warn("Chunk table {} does not exist", tableName);
        }
*/
    }

    @Override
    public void createGlobalOutbox(String tableName) throws SQLException {

    }

    @Override
    public void dropOutboxTable(boolean sync, String tableName) throws SQLException {

    }

    @Override
    public List<Chunk<K, T, S, R>> getChunkList(List<Config> configs, String chunkTableName, Storage<K, T, S, R> targetStorage) throws SQLException {
        return List.of();
    }

    @Override
    public String buildStartEndOfChunk(Config config, String chunkTableName) {
        return "";
    }

    @Override
    public LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        return null;
    }

    @Override
    public String buildFetchStatement(Config config, Table2Table<S> t2t) {
        return "";
    }

    @Override
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?, ?, ?> chunk) {
        return Map.of();
    }

    @Override
    public Table<S> configToTable(String schemaName, String tableName) {
        return new MSSQLTable<>(schemaName, tableName, null);
    }

    @Override
    public void enrichTable(Table<S> sourceTable) throws SQLException {

    }

    @Override
    public void enrichTable(Table<S> sourceTable, Table<S> targetTable) throws SQLException {

    }
}
