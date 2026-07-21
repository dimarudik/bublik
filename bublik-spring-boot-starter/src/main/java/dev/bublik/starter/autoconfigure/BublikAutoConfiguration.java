package dev.bublik.starter.autoconfigure;

import dev.bublik.core.model.Config;
import dev.bublik.core.model.PseudoTable;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageService;
import dev.bublik.starter.properties.BublikProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.stream.Collectors;

@Configuration
@EnableConfigurationProperties(BublikProperties.class)
@ConditionalOnProperty(prefix = "bublik", name = "enabled", havingValue = "true", matchIfMissing = true)
public class BublikAutoConfiguration {
    private static final Logger log = LoggerFactory.getLogger(BublikAutoConfiguration.class);
    private final BublikProperties properties;

    public BublikAutoConfiguration(BublikProperties properties) {
        this.properties = properties;
    }

    @Bean
    public ApplicationRunner automatedMigrationRunner() {
        return args -> {
            if (properties.getPipelines() == null || properties.getPipelines().isEmpty()) {
                log.warn("[Bublik Starter] Active data pipelines configuration list is empty. Execution skipped.");
                return;
            }

            try {
                log.info("[Bublik Starter] Initiating automated data migration process at startup...");

                List<Config> coreConfigs = properties.getPipelines().stream()
                        .map(BublikProperties.TablePipelineConfig::toRecordConfig)
                        .collect(Collectors.toList());

                Table chunkTable = new PseudoTable("public", "bublik_chunks");
                Table outboxTable = new PseudoTable("public", "bublik_outbox");

                long startTime = System.currentTimeMillis();

                StorageService.init(
                        properties.toConnectionProperty(),
                        coreConfigs,
                        10000,
                        chunkTable,
                        outboxTable
                );

                log.info("[Bublik Starter] Migration finished successfully in {} ms.", (System.currentTimeMillis() - startTime));
            } catch (Exception e) {
                log.error("[Bublik Starter] Critical failure inside core replication engine: {}", e.getMessage(), e);
                throw new RuntimeException("Bublik automated migration failed at context startup", e);
            }
        };
    }
}
