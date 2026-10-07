package dev.bublik.kora.config;

import io.koraframework.common.annotation.KoraApp;
import io.koraframework.config.hocon.HoconConfigModule;

@KoraApp
public interface TestApp extends HoconConfigModule, KoraBublikModule {
}
