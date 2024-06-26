package org.bublik.storage;

import com.datastax.driver.core.Cluster;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.model.LogMessage;
import org.bublik.service.StorageService;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class CassandraStorage extends Storage implements StorageService {
    private final Cluster cluster;

    public CassandraStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        this.cluster = Cluster
                .builder()
                .addContactPoint(getStorageClass().getProperties().getProperty("host"))
                .withPort(Integer.parseInt(getStorageClass().getProperties().getProperty("port")))
                .withoutJMXReporting()
                .build();
//        this.session = cluster.connect();
    }

    public Cluster getSource() {
        return cluster;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public void initiateTargetThread(List<Future<LogMessage>> futures, List<Config> configs, ExecutorService executorService) {

    }

/*
    public Session getConn() {
        return session;
    }
*/
}
