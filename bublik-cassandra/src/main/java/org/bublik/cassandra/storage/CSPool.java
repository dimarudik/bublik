package org.bublik.cassandra.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class CSPool {
    private static final Logger log = LoggerFactory.getLogger(CSPool.class);
    private final CqlSession cqlSession;
    private final int size;

    public CSPool(Properties properties, int size) {
        this.size = size;
        this.cqlSession = createCqlSession(properties);
    }

    public CqlSession createCqlSession(Properties properties) {
        return CqlSession
                .builder()
                .addContactPoints(getAddresses(properties))
                .withConfigLoader(getConfigLoader(properties))
                .withAuthCredentials(properties.getProperty("user"), properties.getProperty("password"))
                .withLocalDatacenter(properties.getProperty("datacenter"))
                .build();
    }

    public CqlSession getCqlSession() {
        return cqlSession;
    }

    public List<InetSocketAddress> getAddresses(Properties properties) {
        List<String> hosts = Arrays.asList(properties.getProperty("hosts").split(",", -1));
        return hosts
                .stream()
                .map(h -> new InetSocketAddress(h, Integer.parseInt(properties.getProperty("port"))))
                .toList();
    }

    public DriverConfigLoader getConfigLoader(Properties properties) {
        return DriverConfigLoader
                .programmaticBuilder()
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, size)
                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, size)
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
                        Duration.ofSeconds(14))
                .build();
    }

    public void closeCqlSession() {
        if (cqlSession != null && !cqlSession.isClosed()) {
            cqlSession.close();
        }
    }

    public Set<TokenRange> getTokenRanges() {
        return cqlSession.getMetadata().getTokenMap().orElseThrow().getTokenRanges();
    }
}
