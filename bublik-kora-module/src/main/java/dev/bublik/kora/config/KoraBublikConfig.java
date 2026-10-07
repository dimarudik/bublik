package dev.bublik.kora.config;

import io.koraframework.config.common.annotation.ConfigMapper;

import java.util.List;
import java.util.Map;

@ConfigMapper
public interface KoraBublikConfig {
    int defaultRowsInChunk();
    Map<String, StorageNodeConfig> storages();
    List<MigrationTaskConfig> migrations();
}
