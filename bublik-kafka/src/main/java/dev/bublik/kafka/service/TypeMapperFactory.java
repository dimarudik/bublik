package dev.bublik.kafka.service;

public class TypeMapperFactory {
    private static final AvroTypeMapper INT_MAPPER = (rs, col) -> {
        int val = rs.getInt(col);
        return rs.wasNull() ? null : val;
    };
    private static final AvroTypeMapper LONG_MAPPER = (rs, col) -> {
        long val = rs.getLong(col);
        return rs.wasNull() ? null : val;
    };
    private static final AvroTypeMapper DOUBLE_MAPPER = (rs, col) -> {
        double val = rs.getDouble(col);
        return rs.wasNull() ? null : val;
    };
    private static final AvroTypeMapper FLOAT_MAPPER = (rs, col) -> {
        float val = rs.getFloat(col);
        return rs.wasNull() ? null : val;
    };
    private static final AvroTypeMapper BOOLEAN_MAPPER = (rs, col) -> {
        boolean val = rs.getBoolean(col);
        return rs.wasNull() ? null : val;
    };
    private static final AvroTypeMapper BYTES_MAPPER = (rs, col) -> {
        byte[] bytesVal = rs.getBytes(col);
        return rs.wasNull() ? null : java.nio.ByteBuffer.wrap(bytesVal);
    };
    private static final AvroTypeMapper DEFAULT_MAPPER = (rs, col) -> {
        Object rawObj = rs.getObject(col);
        if (rawObj == null) return null;
        if (rawObj instanceof java.sql.Clob) {
            java.sql.Clob clob = (java.sql.Clob) rawObj;
            return clob.getSubString(1, (int) clob.length());
        }
        return rawObj.toString();
    };

    public static AvroTypeMapper getMapper(String avroType) {
        return switch (avroType.toLowerCase()) {
            case "int" -> INT_MAPPER;
            case "long" -> LONG_MAPPER;
            case "double" -> DOUBLE_MAPPER;
            case "float" -> FLOAT_MAPPER;
            case "boolean" -> BOOLEAN_MAPPER;
            case "bytes" -> BYTES_MAPPER;
            default -> DEFAULT_MAPPER;
        };
    }
}
