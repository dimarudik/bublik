package dev.bublik.cassandra.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;

public class CSPool {
    private static final Logger log = LoggerFactory.getLogger(CSPool.class);
    private final CqlSession cqlSession;
    private final int size;
    private final Set<TokenRange> tokenRanges;
    private final Metadata metadata;
    private final int majorVersion;

    public CSPool(Properties properties, int size) {
        this.size = size;
        this.cqlSession = createCqlSession(properties);
        this.tokenRanges = tokenRanges();
        this.metadata = cqlSession.getMetadata();
        this.majorVersion = Objects.requireNonNull(cqlSession.getMetadata().getNodes().values().iterator().next().getCassandraVersion()).getMajor();
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public Set<TokenRange> getTokenRanges() {
        return tokenRanges;
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
                .map(h -> new InetSocketAddress(h.split(":")[0],
                        (h.split(":").length == 1 ? 9042 : Integer.parseInt(h.split(":")[1])) ))
                .toList();
    }

    public DriverConfigLoader getConfigLoader(Properties properties) {
        return DriverConfigLoader
                .programmaticBuilder()
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, size)
                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, size)
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(14))
                .build();
    }

    public void closeCqlSession() {
        if (cqlSession != null && !cqlSession.isClosed()) {
            cqlSession.close();
        }
    }

    public Set<TokenRange> tokenRanges() {
        return cqlSession.getMetadata().getTokenMap().orElseThrow().getTokenRanges();
    }

    public int getMajorVersion() {
        return majorVersion;
    }
}
