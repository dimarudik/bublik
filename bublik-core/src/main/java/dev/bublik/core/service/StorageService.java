package dev.bublik.core.service;

import dev.bublik.core.model.*;
import dev.bublik.core.storage.AutoColseableStorageClass;
import dev.bublik.core.storage.JDBCStorageClass;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import static dev.bublik.core.constants.CLassConstants.*;
import static dev.bublik.core.constants.Constants.FETCH_SIZE;
import static dev.bublik.core.util.Utils.getStackTrace;

public interface StorageService {
    Logger log = LoggerFactory.getLogger(StorageService.class);

    void start(Storage targetStorage, List<Config> configs, int rows) throws SQLException;
    void createGlobalOutbox() throws SQLException;
    <K, T, S extends AutoCloseable, R, V> void insertColumnValue(List<ColumnValue<V>> columnValues, Chunk<K, T, S, R> chunk) throws SQLException;
    <K, T, S extends AutoCloseable, R, W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) throws SQLException;
    <K, T, S extends AutoCloseable, R> void closeWriter(Chunk<K, T, S, R> chunk, String tableName);
    <K, T, S extends AutoCloseable, R> void flushBuffer(Chunk<K, T, S, R> chunk);
    void insertProcessedChunkInfo(Chunk <?, ?, ?, ?> chunk) throws SQLException;
    boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk) throws SQLException;
    void dropOutboxTable(boolean sync) throws SQLException;
    List<Config> copyConfigs(List<Config> cfgs);
    List<Chunk<?, ?, ?, ?>> getChunkList(List<Config> configs, Storage targetStorage) throws SQLException;
    String buildStartEndOfChunk(Config config, Table sourceTable);
    <K, T, S extends AutoCloseable, R> LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException;
    void closeStorage();
    String buildFetchStatement(Config config, Table2Table t2t);
    Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?, ?, ?> chunk);
    Map<Table, Table> configsToTables(List<Config> configs, Storage targetStorage);
    Table configToTable(String schemaName, String tableName);
    Table getTargetTableBySourceTable(Table table);
    Table getSourceTableByTargetTable(Table table);
    <S extends AutoCloseable> S getPoolConnection() throws SQLException;
    <S extends AutoCloseable> S getSession();
    String getStorageVersion();
    int getStorageMajorVersion();
    <S extends AutoCloseable> void setSession(S session);
    void enrichTable(Table sourceTable) throws SQLException;
    void enrichTable(Table sourceTable, Table targetTable) throws SQLException;
    List<Column2Column> getColumn2Column(Table sourceTable, Table targetTable, Config config);
    Table2Table getTable2Table(Table sourceTable, Table targetTable, List<Column2Column> c2c, Config config);
    Column columnFromAvro(Map<String, Object> avroSchema, String avroFieldName, int position);
    int getFetchSize();

    static Storage getStorage(StorageClass storageClass,
                              Properties properties,
                              ConnectionProperty connectionProperty,
                              Table outboxTable) throws SQLException {
        if (storageClass instanceof AutoColseableStorageClass) {
            Properties props = storageClass.getProperties();
            String className = props.getProperty("class");
            if (className == null || className.isEmpty()) {
                throw new NullPointerException();
            } else {
                return reflectStorage(className, properties, connectionProperty, outboxTable);
            }
        }
        if (storageClass instanceof JDBCStorageClass) {
            Driver driver = DriverManager.getDriver(properties.getProperty("url"));
            return switch (driver.getClass().getName()) {
                case "oracle.jdbc.OracleDriver" ->
                    reflectStorage(ORACLE_STORAGE_CLASS_NAME, properties, connectionProperty, outboxTable);
                case "org.postgresql.Driver", "sdk.humus.HumusDriver" ->
                    reflectStorage(POSTGRES_STORAGE_CLASS_NAME, properties, connectionProperty, outboxTable);
                case "tech.ydb.jdbc.YdbDriver" ->
                    reflectStorage(YDB_STORAGE_CLASS_NAME, properties, connectionProperty, outboxTable);
                case "com.microsoft.sqlserver.jdbc.SQLServerDriver" ->
                    reflectStorage(MSSQL_STORAGE_CLASS_NAME, properties, connectionProperty, outboxTable);
                default -> throw new RuntimeException();
            };
        }
        return null;
    }

    static StorageClass getStorageClass(Properties properties) throws SQLException {
        String className = properties.getProperty("class");
        if (className != null) {
            return new AutoColseableStorageClass(AutoCloseable.class, properties);
        } else {
            return new JDBCStorageClass(Connection.class, properties);
        }
    }

    static Storage reflectStorage(String className,
                                  Properties properties,
                                  ConnectionProperty connectionProperty,
                                  Table outboxTable) {
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor(StorageClass.class,
                    ConnectionProperty.class, Table.class);
            StorageClass storageClass = getStorageClass(properties);
            log.info("Storage class: {} ", className);
            return (Storage) constructor.newInstance(storageClass, connectionProperty, outboxTable);
        } catch (Exception e) {
//            log.error("{}", getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @Deprecated
    static void init(ConnectionProperty property, List<Config> configs, boolean sync, int rows, String table) throws SQLException, IOException {
        throw new RuntimeException("Deprecated");
    }

    static void init(ConnectionProperty property, List<Config> configs, int rows, Table chunkTable) throws SQLException, IOException {
        Table outboxTable = new PseudoTable(chunkTable.getSchemaName(), chunkTable.getTableName() + "_outbox");
        init(property, configs, rows, chunkTable, outboxTable);
    }

    static void init(ConnectionProperty property, List<Config> configs, int rows, Table chunkTable, Table outboxTable) throws SQLException, IOException {
        log.info("Bublik starting...");
        log.info("VERSION : {}", getVersion());
        try {
            log.info("WORKSTATION: {}", InetAddress.getLocalHost().getHostName());
        } catch (Exception e) {
            log.info("Unknown workstation");
        }
        Runtime runtime = Runtime.getRuntime();
        int vCPU = runtime.availableProcessors();
        log.info("===================== CPU ==============================");
        log.info("CPU onboard: {}", vCPU);
        if (vCPU <= 4 && vCPU < property.getThreadCount()) {
            property.setThreadCount(vCPU);
            log.info("Thread count throttled to {}", property.getThreadCount());
        }
        long byteToMb = 1024L * 1024L;
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        log.info("=================== BUBLIK MEMORY INFO ===================");
        log.info("Max Heap Size (-Xmx):   {} MB", maxMemory == Long.MAX_VALUE ? "Unlimited" : maxMemory / byteToMb);
        log.info("Allocated Heap Size:    {} MB", totalMemory / byteToMb);
        log.info("Used Heap Memory:       {} MB", usedMemory / byteToMb);
        log.info("Free Heap Memory:       {} MB", (maxMemory - usedMemory) / byteToMb);
        log.info("==========================================================");

        log.info("THREADS: {}", property.getThreadCount());
        String sourceUrl = property.getFromProperty().getProperty("url");
        String sourceHosts = property.getFromProperty().getProperty("hosts");
        log.info("SOURCE: {}", sourceUrl == null ? sourceHosts : sourceUrl);
        log.info("SOURCE USERNAME: {}", property.getFromProperty().getProperty("user"));
        log.info("SOURCE FETCH_SIZE: {}", property.getFromProperty().getProperty("fetchSize") == null ? FETCH_SIZE : property.getFromProperty().getProperty("fetchSize"));
        String targetUrl = property.getToProperty().getProperty("url");
        String targetHosts = property.getToProperty().getProperty("hosts");
        log.info("TARGET: {}", targetUrl == null ? targetHosts : targetUrl);
        log.info("TARGET USERNAME: {}", property.getToProperty().getProperty("user"));

//        List<Storage<?,?,?,?>> storages = new ArrayList<>();
        ServiceLoader<StorageFactory> loader = ServiceLoader.load(StorageFactory.class);
        for (StorageFactory factory : loader) {
            log.info("Storage factory: {}", factory.getClass().getName());
//            Storage<?, ?, ?, ?> storage = factory.create(property);
//            log.info("Storage: {}", storage.getClass().getName());
        }
/*
        for (Storage<?,?,?,?> storage : loader) {
            storages.add(storage);
        }
        storages.forEach(s -> log.info("Storage: {}", s.getClass().getName()));
*/

/*
        for (ProxyPluginFactory factory : loader) {
            ProxyPlugin plugin = factory.create(url, info);
            if (plugin != null) {
                plugins.add(plugin);
            }
        }
*/
        StorageClass sourceStorageClass = StorageService.getStorageClass(property.getFromProperty());
        StorageClass targetStorageClass = StorageService.getStorageClass(property.getToProperty());
        try (Storage sourceStorage = getStorage(sourceStorageClass, property.getFromProperty(), property, chunkTable);
             Storage targetStorage = getStorage(targetStorageClass, property.getToProperty(), property, outboxTable)) {
            assert sourceStorage != null;
            sourceStorage.start(targetStorage, configs, rows);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static String getVersion() throws IOException {
        try {
            final Properties properties = new Properties();
            properties.load(StorageService.class.getClassLoader().getResourceAsStream("project.properties"));
            return properties.getProperty("version");
        } catch (NullPointerException npe) {
            return "unknown";
        }
    }
}
