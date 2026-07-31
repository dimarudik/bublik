package dev.bublik.cassandra.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
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
    private final Set<TokenRange> tokenRanges;
    private final Metadata metadata;

    public CSPool(CqlSession cqlSession) {
        this.cqlSession = cqlSession;
        this.tokenRanges = tokenRanges();
        this.metadata = cqlSession.getMetadata();
    }

    public CSPool(Properties properties, int size) {
        this.cqlSession = createCqlSession(properties, size);
        this.tokenRanges = tokenRanges();
        this.metadata = cqlSession.getMetadata();
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public Set<TokenRange> getTokenRanges() {
        return tokenRanges;
    }

    public CqlSession getCqlSession() {
        return cqlSession;
    }

    public CqlSession createCqlSession(Properties properties, int size) {
        return CqlSession.builder()
                .addContactPoints(getAddresses(properties))
                .withConfigLoader(getConfigLoader(size))
                .withAuthCredentials(properties.getProperty("user"), properties.getProperty("password"))
                .withLocalDatacenter(properties.getProperty("datacenter"))
                .build();
    }

    public List<InetSocketAddress> getAddresses(Properties properties) {
        List<String> hosts = Arrays.asList(properties.getProperty("hosts").split(",", -1));
        return hosts
                .stream()
                .map(h -> new InetSocketAddress(h.split(":")[0],
                        (h.split(":").length == 1 ? 9042 : Integer.parseInt(h.split(":")[1])) ))
                .toList();
    }

    public DriverConfigLoader getConfigLoader(int size) {
        return DriverConfigLoader.programmaticBuilder()
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, size)
                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, size)
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(14))
                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(14))
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
        return Objects.requireNonNull(cqlSession.getMetadata().getNodes().values().iterator().next().getCassandraVersion()).getMajor();
    }

    public String getVersion() {
        ResultSet rs = cqlSession.execute("SELECT release_version FROM system.local");
        Row row = rs.one();
        return (row != null) ? row.getString("release_version") : "Unknown";
    }

    public int getSize() {
        DriverContext driverContext = cqlSession.getContext();
        return driverContext == null ? 0 :
                driverContext.getConfigLoader().getInitialConfig().getDefaultProfile().getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE);
    }
}
