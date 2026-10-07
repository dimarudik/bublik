package dev.bublik.kora.config;

import io.koraframework.config.common.annotation.ConfigMapper;
import org.jspecify.annotations.Nullable;

@ConfigMapper
public interface StorageNodeConfig {
    String type();
    String url();
    String user();
    String password();
    int threadCount();
    @Nullable Integer fetchSize();
}
