package org.bublik.core.service;

import org.bublik.core.model.Config;

import java.util.List;

public interface JDBCStorageService {
    String buildStartEndOfChunk(List<Config> configs);
}
