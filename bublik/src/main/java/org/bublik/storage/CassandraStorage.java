package org.bublik.storage;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.bublik.model.Chunk;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.model.LogMessage;
import org.bublik.service.ChunkService;
import org.bublik.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class CassandraStorage extends Storage implements StorageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStorage.class);
    private final Cluster cluster;
    private final Session session;

    public CassandraStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        this.cluster = Cluster
                .builder()
                .addContactPoint(getStorageClass().getProperties().getProperty("host"))
                .withPort(Integer.parseInt(getStorageClass().getProperties().getProperty("port")))
                .withoutJMXReporting()
                .build();
        this.session = cluster.connect();
    }

    public Cluster getCluster() {
        return cluster;
    }

    @Override
    public void startWorker(List<Future<LogMessage>> futures, List<Config> configs, ExecutorService executorService) throws SQLException {

    }

    @Override
    public LogMessage callWorker(Chunk<?> chunk, Map<String, Integer> columnsFromDB) throws SQLException {
        return null;
    }

    @Override
    public LogMessage transferToTarget(ResultSet resultSet) throws SQLException {
        Chunk<?> chunk = ChunkService.get();
        long start = System.currentTimeMillis();
        return new LogMessage(
                0,
                start,
                "Cassandra BATCH APPLY",
                chunk);
    }

    @Override
    public void closeStorage() {
        session.close();
        cluster.close();
    }
}
