package dev.bublik.clickhouse.storage;

import com.clickhouse.client.api.Client;
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

public abstract class ClickStorage<K, T, S extends Client, R> extends Storage<K, T, S, R> implements Source {
    protected final int threadCount;
    private final ClickClient clickClient;

    protected ClickStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        this.threadCount = connectionProperty.getThreadCount();
        this.clickClient = new ClickClient(getStorageClass().getProperties(), connectionProperty.getThreadCount());
    }

    @Override
    public void fulfillChunks(List<Config> configs, boolean sync, int rows, String tableName) throws SQLException {

    }

    @Override
    public void dropChunkTable(List<Config> configs, boolean sync, String tableName) throws SQLException {

    }

    @Override
    public void start(List<Config> configs, boolean sync, int rows, Storage<K, T, S, R> targetStorage, String tableName, Storage<K, T, S, R> cacheStorage) throws SQLException {

    }

    @Override
    public void createGlobalOutbox(String tableName) throws SQLException {

    }

    @Override
    public <V, W> void insertColumnValue(List<ColumnValue<V>> columnValues, Chunk<K, T, S, R> chunk, W writer) throws SQLException {

    }

    @Override
    public <W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) throws SQLException, SourceSQLException, IOException {
        return null;
    }

    @Override
    public <W> void closeWriter(W writer, Chunk<K, T, S, R> chunk, String tableName) throws SQLException {

    }

    @Override
    public void insertProcessedChunkInfo(Chunk<?, ?, ?, ?> chunk, String tableName) throws SQLException {

    }

    @Override
    public boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk, String tableName) throws SQLException {
        return false;
    }

    @Override
    public void dropOutboxTable(boolean sync, String tableName) throws SQLException {

    }

    @Override
    public List<Config> copyConfigs(List<Config> cfgs) {
        return List.of();
    }

    @Override
    public List<Chunk<K, T, S, R>> getChunkList(List<Config> configs, String chunkTableName, Storage<K, T, S, R> targetStorage) throws SQLException {
        return List.of();
    }

    @Override
    public String buildStartEndOfChunk(Config config, String chunkTableName, Table<S> sourceTable) {
        return "";
    }

    @Override
    public void closeStorage() {

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
    public Map<Table<S>, Table<S>> configsToTables(List<Config> configs, Storage<K, T, S, R> targetStorage) {
        return Map.of();
    }

    @Override
    public Table<S> configToTable(String schemaName, String tableName) {
        return null;
    }

    @Override
    public Table<S> getTagetTableBySourceTable(Table<S> table) {
        return null;
    }

    @Override
    public Table<S> getSourceTableByTargetTable(Table<S> table) {
        return null;
    }

    @Override
    public S getPoolConnection() throws SQLException {
        return null;
    }

    @Override
    public S getSession() {
        return null;
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
    public void setSession(S session) {

    }

    @Override
    public void enrichTable(Table<S> sourceTable) throws SQLException {

    }

    @Override
    public void enrichTable(Table<S> sourceTable, Table<S> targetTable) throws SQLException {

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
