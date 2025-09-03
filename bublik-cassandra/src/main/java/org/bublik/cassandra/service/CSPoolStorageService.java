package org.bublik.cassandra.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface CSPoolStorageService {
    CqlSession createCqlSession();
    CqlSession getCqlSession();
    void freeCqlSession(CqlSession cqlSession);
    void closeCqlSession(CqlSession cqlSession);
    List<InetSocketAddress> getAddresses(Properties properties);
    DriverConfigLoader getConfigLoader(Properties properties);
    Map<CqlSession, Short> initSessionMap(int threadCount);
}
