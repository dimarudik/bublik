package org.bublik.cassandra.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.bublik.cassandra.service.CSPoolStorageService;
import org.bublik.core.model.*;
import org.bublik.core.storage.Storage;
import org.bublik.core.storage.StorageClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public abstract class CSPoolStorage extends Storage implements CSPoolStorageService {
    private static final Logger log = LoggerFactory.getLogger(CSPoolStorage.class);
    private static final ReentrantLock lock = new ReentrantLock();

    private final int batchSize;
    private final Map<CqlSession, Short> sessionMap;

    protected CSPoolStorage(StorageClass storageClass,
                            ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        this.batchSize = getBatchSize(connectionProperty);
        sessionMap = initSessionMap(connectionProperty.getThreadCount());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        closeStorage();
    }

    public int getBatchSize(ConnectionProperty connectionProperty) {
        String batchSize = connectionProperty.getToProperty().getProperty("batchSize");
        return  batchSize == null ? 100 : Integer.parseInt(batchSize);
    }

    public Map<CqlSession, Short> getSessionMap() {
        return sessionMap;
    }

    @Override
    public Map<CqlSession, Short> initSessionMap(int threadCount) {
        Map<CqlSession, Short> sessionMap = new ConcurrentHashMap<>();
        for (short i = 0; i < threadCount; i++) {
            sessionMap.put(createCqlSession(), (short) 0);
        }
        return sessionMap;
    }

    @Override
    public CqlSession createCqlSession() {
        Properties properties = getStorageClass().getProperties();
        return CqlSession
                .builder()
                .addContactPoints(getAddresses(properties))
                .withConfigLoader(getConfigLoader(properties))
                .withAuthCredentials(properties.getProperty("user"), properties.getProperty("password"))
                .withLocalDatacenter(properties.getProperty("datacenter"))
                .build();
    }

    @Override
    public CqlSession getCqlSession() {
        CqlSession cqlSession;
        lock.lock();
        try {
             cqlSession = getSessionMap()
                    .entrySet()
                    .stream()
                    .filter(e -> e.getValue() == 0)
                    .findFirst()
                    .map(this::lockSession)
                    .map(Map.Entry::getKey)
                    .orElseThrow();
//            getSessionMap().put(cqlSession, (short) 1);
        } finally {
            lock.unlock();
        }
        return cqlSession;
    }

    private Map.Entry<CqlSession, Short> lockSession(Map.Entry<CqlSession, Short> entry) {
        entry.setValue((short) 1);
        return entry;
    }

    @Override
    public List<InetSocketAddress> getAddresses(Properties properties) {
        List<String> hosts = Arrays.asList(properties.getProperty("hosts").split(",", -1));
        return hosts
                .stream()
                .map(h -> new InetSocketAddress(h, Integer.parseInt(properties.getProperty("port"))))
                .toList();
    }

    @Override
    public DriverConfigLoader getConfigLoader(Properties properties) {
        return DriverConfigLoader
                .programmaticBuilder()
//                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 8)
//                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 8)
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
                        Duration.ofSeconds(Long.parseLong(properties.getProperty("query_time_out"))))
                .build();
    }

    @Override
    public void freeCqlSession(CqlSession cqlSession) {
        getSessionMap().put(cqlSession, (short) 0);
/*
        getSessionMap()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().equals(cqlSession))
                .findFirst()
                .ifPresent(this::unLockSession);
*/
    }

    @Override
    public void closeCqlSession(CqlSession cqlSession) {
        if (cqlSession != null && !cqlSession.isClosed()) {
            cqlSession.close();
        }
    }

    @Override
    public void start(List<Config> configs, boolean sync, int rows, Storage targetStorage) throws SQLException {

    }

    @Override
    public void createChunks(Connection connection, List<Config> configs, boolean sync, int rows) throws SQLException {

    }

    @Override
    public void createOutbox() throws SQLException {

    }

    @Override
    public List<Chunk<?>> getChunkList(List<Config> configs, Connection connection) throws SQLException {
        return List.of();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public void closeStorage() {
        for (CqlSession cqlSession : sessionMap.keySet()) {
            closeCqlSession(cqlSession);
        }
    }

    @Override
    public String buildFetchStatement(Config config, Table sourceTable) {
        return buildFetchStatement(config);
    }

    @Override
    public String buildFetchStatement(Config config) {
        return "";
    }

    @Override
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?> chunk) {
        return Map.of();
    }

    @Override
    public Map<Table, Table> configsToTables(List<Config> configs, Storage targetStorage) {
        return Map.of();
    }

    @Override
    public Table configToTable(String schemaName, String tableName) {
        return null;
    }

    @Override
    public Table getTagetTableBySourceTable(Table table) {
        return null;
    }

    @Override
    public Table getSourceTableByTargetTable(Table table) {
        return null;
    }

/*
    @Override
    public void enrichSourceTables() {

    }

    @Override
    public void enrichTargetTables(Map<Table, Table> tables) {

    }
*/

    @Override
    public <T> T unwrap(Class<T> iface) {
        if (iface.isInstance(this)) {
            return (T) this;
        } else {
            throw new RuntimeException("No object found that implements the interface: " + iface.getName());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
