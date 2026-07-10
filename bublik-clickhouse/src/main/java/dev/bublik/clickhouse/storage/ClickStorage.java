package dev.bublik.clickhouse.storage;

import com.clickhouse.client.api.Client;
import dev.bublik.clickhouse.model.ClickTable;
import dev.bublik.core.exception.SourceSQLException;
import dev.bublik.core.model.*;
import dev.bublik.core.service.Source;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static dev.bublik.core.constants.Constants.CHUNK_TABLE_NAME;

abstract class ClickStorage extends Storage implements Source {
    protected ClickClient clickClient;

    public ClickStorage(Client client, Table outboxTable) {
        super(new ConnectionProperty(), outboxTable);
        ClickClient clickClient = new ClickClient(client);
        this.clickClient = clickClient;
        this.threadCount = clickClient.getSize();
    }

    public ClickStorage(Client client, int threadCount, Table outboxTable) {
        super(new ConnectionProperty(), outboxTable);
        this.threadCount = threadCount;
        this.clickClient = new ClickClient(client);
    }

    public ClickStorage(StorageClass storageClass,
                        ConnectionProperty connectionProperty,
                        Table outboxTable) {
        super(storageClass, connectionProperty, outboxTable);
        this.threadCount = connectionProperty.getThreadCount();
        this.clickClient = new ClickClient(getStorageClass().getProperties(), connectionProperty.getThreadCount());
    }

    @Override
    public void fulfillChunks(List<Config> configs, boolean sync, int rows) throws SQLException {

    }

    @Override
    public void dropChunkTable(List<Config> configs, boolean sync) throws SQLException {

    }

    @Override
    public void start(Storage targetStorage, List<Config> configs, int rows) throws SQLException {
        start(targetStorage, configs, rows, false);
    }

    @Override
    public void start(Storage targetStorage, List<Config> configs, int rows, boolean sync) throws SQLException {

    }

    @Override
    public void createGlobalOutbox() throws SQLException {

    }

    @Override
    public <K, T, S extends AutoCloseable, R, V>  void insertColumnValue(List<ColumnValue<V>> columnValues, Chunk<K, T, S, R> chunk) throws SQLException {

    }

    @Override
    public <K, T, S extends AutoCloseable, R, W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        return null;
    }

    @Override
    public <K, T, S extends AutoCloseable, R>  void closeWriter(Chunk<K, T, S, R> chunk, String tableName) {

    }

    @Override
    public void insertProcessedChunkInfo(Chunk<?, ?, ?, ?> chunk) throws SQLException {

    }

    @Override
    public boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk) throws SQLException {
        return false;
    }

    @Override
    public void dropOutboxTable(boolean sync) throws SQLException {

    }

    @Override
    public List<Config> copyConfigs(List<Config> cfgs) {
        return List.of();
    }

    @Override
    public List<Chunk<?, ?, ?, ?>> getChunkList(List<Config> configs, Storage targetStorage) throws SQLException {
        return List.of();
    }

    @Override
    public String buildStartEndOfChunk(Config config, Table sourceTable) {
        return "";
    }

    @Override
    public void closeStorage() {

    }

    @Override
    public String buildFetchStatement(Config config, Table2Table t2t) {
        return "";
    }

    @Override
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?, ?, ?> chunk) {
        return Map.of();
    }

    @Override
    public Map<Table, Table> configsToTables(List<Config> configs, Storage targetStorage) {
        return Map.of();
    }

    @Override
    public Table configToTable(String schemaName, String tableName) {
        return new ClickTable(schemaName, tableName);
    }

    @Override
    public Table getTargetTableBySourceTable(Table table) {
        return null;
    }

    @Override
    public Table getSourceTableByTargetTable(Table table) {
        return null;
    }

    @Override
    public <S extends AutoCloseable> S getPoolConnection() throws SQLException {
        return null;
    }

    @Override
    public <S extends AutoCloseable> S getSession() {
        return (S) clickClient.getClient();
    }

    @Override
    public String getStorageVersion() {
        return "";
    }

    @Override
    public int getStorageMajorVersion() {
        return 0;
    }

    @Override
    public <S extends AutoCloseable>void setSession(S session) {

    }

    @Override
    public void enrichTable(Table sourceTable) throws SQLException {

    }

    @Override
    public void enrichTable(Table sourceTable, Table targetTable) throws SQLException {
        targetTable.enrichTable(getSession());
    }

    @Override
    public List<Column2Column> getColumn2Column(Table sourceTable, Table targetTable, Config config) {
        return List.of();
    }

    @Override
    public Table2Table getTable2Table(Table sourceTable, Table targetTable, List<Column2Column> c2c, Config config) {
        return null;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
