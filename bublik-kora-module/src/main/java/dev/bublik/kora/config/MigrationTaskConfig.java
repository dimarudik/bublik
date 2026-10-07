package dev.bublik.kora.config;

import io.koraframework.config.common.annotation.ConfigMapper;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;

@ConfigMapper
public interface MigrationTaskConfig {
    String fromSchemaName();
    String fromTableName();
    @Nullable String fromPartitionName();
    @Nullable String fromSubpartitionName();
    @Nullable String fromTableAlias();
    @Nullable String fromTableAdds();
    @Nullable String toSchemaName();
    @Nullable String toTableName();
    @Nullable String fetchHintClause();
    @Nullable String fetchWhereClause();
    @Nullable String fromTaskName();
    @Nullable String fromTaskWhereClause();
    @Nullable String timestamp();
    @Nullable String withTTL();

    @Nullable List<String> tryCharIfAny();
    @Nullable Map<String, String> columnToColumn();
    @Nullable Map<String, String> expressionToColumn();
    @Nullable Map<String, Object> avroSchema();
    @Nullable Map<String, String> validationRules();
}
