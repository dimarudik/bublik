package dev.bublik.core.cache;

import java.sql.Types;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Глобальный менеджер кэшей для хранения значений из PostgreSQL.
 * Поддерживает создание и управление несколькими кэшами в рантайме.
 * Тип значения (Value) теперь динамический (Object).
 */
public class CacheHolder {

    /** Внутренний класс-обёртка для одного кэша */
    public static class CacheInstance {
        // Ключ всегда Long, значение - Object (любой тип из БД)
        private final Map<Long, Object> cache = new ConcurrentHashMap<>();
        private String sourceColumnToCacheKey;

        // Опционально: храним SQL тип данных (из Types), если нужно знать тип при чтении
        private int valueTypeCode = Types.OTHER;

        public Map<Long, Object> getCache() {
            return cache;
        }

        public void clear() {
            cache.clear();
            sourceColumnToCacheKey = null;
            valueTypeCode = Types.OTHER;
        }

        /**
         * Получить значение по ключу.
         * Возвращает Object, который нужно привести к ожидаемому типу.
         */
        public Object get(Long key) {
            return cache.get(key);
        }

        /**
         * Положить значение в кэш.
         */
        public void put(Long key, Object value) {
            cache.put(key, value);
        }

        public int size() {
            return cache.size();
        }

        public void setSourceColumnToCacheKey(String columnName) {
            sourceColumnToCacheKey = columnName;
        }

        public String getSourceColumnToCacheKey() {
            return sourceColumnToCacheKey;
        }

        /**
         * Установить SQL тип данных для значений в этом кэше.
         * Используется константа из java.sql.Types (например, Types.TIMESTAMP, Types.VARCHAR).
         */
        public void setValueTypeCode(int typeCode) {
            this.valueTypeCode = typeCode;
        }

        /**
         * Получить SQL тип данных значений.
         */
        public int getValueTypeCode() {
            return valueTypeCode;
        }
    }

    /** Статическая карта всех кэшей: ключ (имя) -> экземпляр кэша */
    private static final Map<String, CacheInstance> caches = new ConcurrentHashMap<>();

    /**
     * Получить или создать кэш по имени.
     */
    public static CacheInstance getOrCreateCache(String cacheName) {
        return caches.computeIfAbsent(cacheName, name -> new CacheInstance());
    }

    /**
     * Получить кэш по имени (вернёт null, если не существует).
     */
    public static CacheInstance getCache(String cacheName) {
        return caches.get(cacheName);
    }

    /**
     * Удалить кэш по имени.
     */
    public static void removeCache(String cacheName) {
        caches.remove(cacheName);
    }

    /**
     * Очистить все кэши.
     */
    public static void clearAll() {
        caches.clear();
    }

    /**
     * Получить количество активных кэшей.
     */
    public static int getCacheCount() {
        return caches.size();
    }

    public static boolean cacheExists(String cacheName) {
        return caches.containsKey(cacheName);
    }
}