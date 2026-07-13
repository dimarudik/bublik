package dev.bublik.kafka.service;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

public class ObjectTypeMapperFactory {
    private static final ObjectTypeMapper BYTES_MAPPER = raw ->
            (raw instanceof byte[]) ? ByteBuffer.wrap((byte[]) raw) : raw;

    private static final ObjectTypeMapper DEFAULT_MAPPER = raw ->
            (raw != null) ? raw.toString() : null;

    public static ObjectTypeMapper getMapper(String avroType) {
        return switch (avroType.toLowerCase()) {
            case "int", "long", "double", "float", "boolean" -> raw -> raw;
            case "bytes" -> BYTES_MAPPER;

            case "bool" -> raw -> {
                if (raw instanceof Boolean b) return b;
                if (raw instanceof Number n) return n.intValue() != 0;
                return raw != null && Boolean.parseBoolean(raw.toString().trim());
            };

            case "uuid" -> raw -> (raw != null) ? raw.toString().trim() : null;

            case "inet" -> raw -> {
                if (raw instanceof InetAddress addr) return addr.getHostAddress();
                return (raw != null) ? raw.toString().trim() : null;
            };

            case "date" -> raw -> {
                if (raw instanceof LocalDate ld) return ld.toString(); // "YYYY-MM-DD"
                if (raw instanceof java.sql.Date sd) return sd.toLocalDate().toString();
                return (raw != null) ? raw.toString().trim() : null;
            };

            case "time" -> raw -> {
                if (raw instanceof LocalTime lt) return lt.toString(); // "HH:MM:SS.nnn"
                return (raw != null) ? raw.toString().trim() : null;
            };

            case "timestamp" -> raw -> {
                if (raw instanceof Instant inst) return inst.toString(); // "YYYY-MM-DDTHH:MM:SS.Z"
                if (raw instanceof java.sql.Timestamp ts) return ts.toInstant().toString();
                if (raw instanceof Long l) return Instant.ofEpochMilli(l).toString();
                return (raw != null) ? raw.toString().trim() : null;
            };

            default -> DEFAULT_MAPPER;
        };
    }
}
