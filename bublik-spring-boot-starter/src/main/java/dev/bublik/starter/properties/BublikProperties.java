package dev.bublik.starter.properties;

import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@ConfigurationProperties(prefix = "bublik")
public class BublikProperties {
    private boolean enabled = true;
    private TriggerMode triggerMode = TriggerMode.ON_START;
    private String cron;
    private int threadCount = 4;
    private Boolean rowsStat = false;
    private Map<String, String> from = new HashMap<>();
    private Map<String, String> to = new HashMap<>();
    private Map<String, String> crypto = new HashMap<>();
    private Map<String, Map<String, String>> toAdds = new HashMap<>();
    private List<TablePipelineConfig> pipelines;

    public enum TriggerMode {
        ON_START,
        SCHEDULED
    }

    public ConnectionProperty toConnectionProperty() {
        return new ConnectionProperty(
                this.threadCount,
                this.from,
                this.to,
                this.crypto,
                this.toAdds
        );
    }

    @Data
    public static class TablePipelineConfig {
        private String fromSchema;
        private String fromTable;
        private String toSchema;
        private String toTable;
        private String topic;
        private String withTtl;

        public Config toRecordConfig() {
            Config.Builder builder = Config.builder()
                    .from(fromSchema, fromTable);

            if (topic != null && !topic.isBlank()) {
                builder.to(topic);
            } else {
                builder.to(toSchema, toTable);
            }

            if (withTtl != null && !withTtl.isBlank()) {
                builder.withTTL(withTtl);
            }

            return builder.build();
        }
    }
}
