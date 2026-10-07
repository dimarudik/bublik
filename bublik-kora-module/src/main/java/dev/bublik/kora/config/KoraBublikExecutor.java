package dev.bublik.kora.config;

import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.service.StorageService;
import io.koraframework.common.annotation.Component;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Component
public final class KoraBublikExecutor {
    private final ConnectionProperty connectionProperty;
    private final List<Config> configs;
    private final int defaultRowsInChunk;

    // Kora автоматически вызовет этот конструктор, передав сюда готовый конфиг!
    public KoraBublikExecutor(KoraBublikConfig config) {
        System.out.println(this.getClass().getName() + " is created");
        var fromNode = config.storages().get("from");
        var toNode = config.storages().get("to");

        if (fromNode == null || toNode == null) {
            throw new IllegalArgumentException("В конфигурации bublik.storages должны быть объявлены узлы 'from' и 'to'");
        }

        this.connectionProperty = ConnectionProperty.builder()
                .threadCount(fromNode.threadCount())
                .addFromProperty("url", fromNode.url())
                .addFromProperty("user", fromNode.user())
                .addFromProperty("password", fromNode.password())
                .addToProperty("url", toNode.url())
                .addToProperty("user", toNode.user())
                .addToProperty("password", toNode.password())
                .build();

        if (fromNode.fetchSize() != null) {
            this.connectionProperty.getFromProperties().put("fetchSize", String.valueOf(fromNode.fetchSize()));
        }
        if (toNode.fetchSize() != null) {
            this.connectionProperty.getToProperties().put("fetchSize", String.valueOf(toNode.fetchSize()));
        }

        this.configs = new ArrayList<>();
        if (config.migrations() != null) {
            for (MigrationTaskConfig m : config.migrations()) {
                Config coreCfg = Config.builder()
                        .from(m.fromSchemaName(), m.fromTableName())
                        .to(m.toSchemaName(), m.toTableName())
                        .fromPartition(m.fromPartitionName())
                        .fromSubpartition(m.fromSubpartitionName())
                        .fromTableAlias(m.fromTableAlias())
                        .fromTableAdds(m.fromTableAdds())
                        .fetchHintClause(m.fetchHintClause())
                        .fetchWhereClause(m.fetchWhereClause())
                        .fromTaskName(m.fromTaskName())
                        .fromTaskWhereClause(m.fromTaskWhereClause())
                        .timestamp(m.timestamp())
                        .withTTL(m.withTTL())
                        .tryCharIfAny(m.tryCharIfAny())
                        .columnToColumn(m.columnToColumn())
                        .expressionToColumn(m.expressionToColumn())
                        .avroSchema(m.avroSchema())
                        .validationRules(m.validationRules())
                        .build();
                this.configs.add(coreCfg.copy());
            }
        }
        this.defaultRowsInChunk = config.defaultRowsInChunk();
    }

    public void runMigration() throws SQLException, IOException {
        StorageService.init(connectionProperty, configs, defaultRowsInChunk);
    }

    public ConnectionProperty getConnectionProperty() {
        return connectionProperty;
    }

    public List<Config> getConfigs() {
        return configs;
    }
}
