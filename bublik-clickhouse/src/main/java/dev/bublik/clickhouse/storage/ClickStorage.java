package dev.bublik.clickhouse.storage;

import com.clickhouse.client.api.Client;
import dev.bublik.clickhouse.model.ClickTable;
import dev.bublik.core.model.*;
import dev.bublik.core.service.Source;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

abstract class ClickStorage extends Storage implements Source {
    protected ClickClient clickClient;

    public ClickStorage(StorageClass storageClass,
                        ConnectionProperty connectionProperty,
                        Table outboxTable) {
        super(storageClass, connectionProperty, outboxTable);
        this.threadCount = connectionProperty.getThreadCount();
        this.clickClient = new ClickClient(getStorageClass().getProperties(), connectionProperty.getThreadCount());
    }

    protected ClickStorage(ClickStorage.Builder<?, ?> builder) {
        super(builder);
        if (builder.client != null) {
            this.clickClient = new ClickClient(builder.client);
            if (threadCount <= 0) {
                this.threadCount = clickClient.getSize();
            }
        }
    }

    static abstract class Builder<C extends ClickStorage, B extends Builder<C, B>>
            extends Storage.Builder<C, B> {
        protected final Client client;

        public Builder(Client client) {
            this.client = client;
        }
    }

    @Override
    public void validate(Storage targetStorage, List<Config> configs) throws SQLException {

    }

    @Override
    public void createChunkTable() throws SQLException {

    }

    @Override
    public void fulfillChunks(List<Config> configs, boolean sync, int rows) throws SQLException {

    }

    @Override
    public void dropChunkTable(List<Config> configs) throws SQLException {

    }

    @Override
    public void start(Storage targetStorage, List<Config> configs, int rows) throws SQLException {

    }

    @Override
    public void createGlobalOutbox() throws SQLException {

    }

    @Override
    public <K, T, S extends AutoCloseable, R, W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) {
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
    public List<Chunk<?, ?, ?, ?>> getChunkList(List<TableMigrationContext> contexts, Storage targetStorage) throws SQLException {
        return List.of();
    }

    @Override
    public Chunk<?, ?, ?, ?> getChunk(ResultSet rs, TableMigrationContext ctx, Storage targetStorage) throws SQLException {
        return null;
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
        Client rawClient = clickClient.getClient();
        var record = rawClient.queryAll("SELECT version()").getFirst();
        return record.getString(1);
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

    @Override
    public void preChecks(List<Config> configs) throws SQLException {

    }
}
