package org.bublik.service;

import org.bublik.model.Chunk;
import org.bublik.model.Config;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface JDBCStorageService {
    String buildStartEndOfChunk(List<Config> configs);
}
