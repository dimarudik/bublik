package org.bublik.service;

import com.datastax.oss.driver.api.core.CqlSession;
import org.bublik.model.Chunk;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.model.LogMessage;
import org.bublik.storage.*;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface StorageService {
//    ThreadLocal<Storage> STORAGE_THREAD_LOCAL = new ThreadLocal<>();

    void start(List<Config> configs) throws SQLException;
//    boolean hook(List<Config> configs) throws SQLException;
    Map<Integer, Chunk<?>> getChunkMap(List<Config> configs) throws SQLException;
    Connection getConnection() throws SQLException;
    LogMessage transferToTarget(Chunk<?> chunk) throws SQLException;
    void closeStorage();

/*
    static void set(Storage storage) {
        STORAGE_THREAD_LOCAL.set(storage);
    }

    static Storage get() {
        return STORAGE_THREAD_LOCAL.get();
    }

    static void remove() {
        STORAGE_THREAD_LOCAL.remove();
    }
*/

    static Storage getStorage(Properties properties, ConnectionProperty connectionProperty, Boolean isSource) throws SQLException {
        StorageClass storageClass = StorageService.getStorageClass(properties);
        if (storageClass instanceof CassandraStorageClass) {
            return new CassandraStorage(storageClass, connectionProperty, isSource);
        }
        if (storageClass instanceof JDBCStorageClass) {
            Driver driver = DriverManager.getDriver(properties.getProperty("url"));
            return switch (driver.getClass().getName()) {
                case "oracle.jdbc.OracleDriver" -> JDBCOracleStorage.getInstance(storageClass, connectionProperty, isSource);
                case "org.postgresql.Driver" -> JDBCPostgreSQLStorage.getInstance(storageClass, connectionProperty, isSource);
                default -> throw new RuntimeException();
            };
        }
        return null;
    }

    static StorageClass getStorageClass(Properties properties) throws SQLException {
        String storageType = properties.getProperty("type");
        if (storageType != null) {
            return switch (storageType) {
                case "cassandra" -> new CassandraStorageClass(CqlSession.class, properties);
//                case "ydb" -> new YdbTransportImpl.class;
                default -> throw new RuntimeException("Unknown storage type");
            };
        } else {
            Driver driver = DriverManager.getDriver(properties.getProperty("url"));
            return new JDBCStorageClass(Connection.class, properties);
        }

/*
        try {
            Driver driver = DriverManager.getDriver(properties.getProperty("url"));
            return Connection.class;
        } catch (SQLException | NullPointerException e) {
            try (GrpcTransport grpcTransport = GrpcTransport
                    .forHost(properties.getProperty("host"),
                            Integer.parseInt(properties.getProperty("port")),
                            properties.getProperty("database"))
                    .build()) {
                return grpcTransport.getClass();
            } catch (RuntimeException ex) {
                try (Cluster cluster = Cluster
                        .builder()
                        .addContactPoint(properties.getProperty("host"))
                        .withPort(Integer.parseInt(properties.getProperty("port")))
                        .withoutJMXReporting()
                        .build();
                     Session session = cluster.connect()) {
                    return cluster.getClass();
                } catch (NoHostAvailableException exNoHost) {
                    System.out.println("Here...");
                }
            }
        }
        throw new RuntimeException();
*/
    }
}
