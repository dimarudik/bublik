package dev.bublik.kafka.service;

import java.nio.ByteBuffer;

public class ObjectTypeMapperFactory {
    private static final ObjectTypeMapper BYTES_MAPPER = raw ->
            (raw instanceof byte[]) ? ByteBuffer.wrap((byte[]) raw) : raw;

    private static final ObjectTypeMapper DEFAULT_MAPPER = raw ->
            (raw != null) ? raw.toString() : null;

    public static ObjectTypeMapper getMapper(String avroType) {
        return switch (avroType.toLowerCase()) {
            case "int", "long", "double", "float", "boolean" -> raw -> raw;
            case "bytes" -> BYTES_MAPPER;
            default -> DEFAULT_MAPPER;
        };
    }
}
