package dev.bublik.kora.config;

import io.koraframework.common.annotation.Module;
import io.koraframework.config.common.mapper.ConfigValueMapper;
import io.koraframework.config.common.Config;

@Module
public interface KoraBublikModule {
    default KoraBublikConfig koraBublikConfig(Config config, ConfigValueMapper<KoraBublikConfig> mapper) {
        return mapper.mapOrThrow(config.get("bublik"));
    }
}
