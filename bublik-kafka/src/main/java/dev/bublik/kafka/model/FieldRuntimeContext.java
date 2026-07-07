package dev.bublik.kafka.model;

import dev.bublik.kafka.service.AvroTypeMapper;

public record FieldRuntimeContext(
        String avroFieldName,
        String dbLookupName,
        AvroTypeMapper mapper
) {
}
