package org.bublik.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Properties;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SourceTargetProperties {
    private int threadCount;
    @Builder.Default
    private Boolean initPGChunks = true;
    @Builder.Default
    private Boolean copyPGChunks = true;
    private Map<String, String> fromProperties;
    private Map<String, String> toProperties;

    public Properties getFromProperty() {
        return getProperties(fromProperties);
    }

    public Properties getToProperty() {
        return getProperties(toProperties);
    }

    private Properties getProperties(Map<String, String> map) {
        Properties properties = new Properties();
        properties.putAll(map);
        return properties;
    }
}
