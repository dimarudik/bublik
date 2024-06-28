package org.bublik.service;

import com.datastax.driver.core.Cluster;
import org.bublik.model.Chunk;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.model.LogMessage;
import org.bublik.storage.*;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public interface StorageService {
    ThreadLocal<Storage> STORAGE_THREAD_LOCAL = new ThreadLocal<>();

    void startWorker(List<Future<LogMessage>> futures, List<Config> configs, ExecutorService executorService) throws SQLException;
    LogMessage callWorker(Chunk<?> chunk, Map<String, Integer> columnsFromDB) throws SQLException;
    LogMessage transferToTarget(ResultSet resultSet) throws SQLException;
    void closeStorage();

    static void set(Storage storage) {
        STORAGE_THREAD_LOCAL.set(storage);
    }

    static Storage get() {
        return STORAGE_THREAD_LOCAL.get();
    }

    static void remove() {
        STORAGE_THREAD_LOCAL.remove();
    }

    static Storage getStorage(Properties properties, ConnectionProperty connectionProperty) throws SQLException {
        StorageClass storageClass = StorageService.getStorageClass(properties);
        if (storageClass instanceof CassandraStorageClass) {
            return new CassandraStorage(storageClass, connectionProperty);
        }
        if (storageClass instanceof JDBCStorageClass) {
            Driver driver = DriverManager.getDriver(properties.getProperty("url"));
            return switch (driver.getClass().getName()) {
                case "oracle.jdbc.OracleDriver" -> new JDBCOracleStorage(storageClass, connectionProperty);
                case "org.postgresql.Driver" -> new JDBCPostgreSQLStorage(storageClass, connectionProperty);
                default -> throw new RuntimeException();
            };
        }
        return null;
    }

    static StorageClass getStorageClass(Properties properties) throws SQLException {
        String storageType = properties.getProperty("type");
        if (storageType != null) {
            return switch (storageType) {
                case "cassandra" -> new CassandraStorageClass(Cluster.class, properties);
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
