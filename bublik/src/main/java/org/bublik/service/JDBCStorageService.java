package org.bublik.service;

import org.bublik.model.Config;

import java.util.List;

public interface JDBCStorageService {
    String buildStartEndOfChunk(List<Config> configs);
    String buildFetchStatement(Config config);
}
