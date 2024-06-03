package org.bublik.model;

import java.util.Map;
import java.util.Properties;

//@Data
//@Builder
//@AllArgsConstructor
//@NoArgsConstructor
public class SourceTargetProperties {
    private int threadCount;
//    @Builder.Default
    private Boolean initPGChunks = true;
//    @Builder.Default
    private Boolean copyPGChunks = true;
    private Map<String, String> fromProperties;
    private Map<String, String> toProperties;

    public SourceTargetProperties(){}
    public SourceTargetProperties(int threadCount, Boolean initPGChunks, Boolean copyPGChunks, Map<String, String> fromProperties, Map<String, String> toProperties) {
        this.threadCount = threadCount;
        this.initPGChunks = initPGChunks;
        this.copyPGChunks = copyPGChunks;
        this.fromProperties = fromProperties;
        this.toProperties = toProperties;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public Boolean getInitPGChunks() {
        return initPGChunks;
    }

    public void setInitPGChunks(Boolean initPGChunks) {
        this.initPGChunks = initPGChunks;
    }

    public Boolean getCopyPGChunks() {
        return copyPGChunks;
    }

    public void setCopyPGChunks(Boolean copyPGChunks) {
        this.copyPGChunks = copyPGChunks;
    }

    public Map<String, String> getFromProperties() {
        return fromProperties;
    }

    public void setFromProperties(Map<String, String> fromProperties) {
        this.fromProperties = fromProperties;
    }

    public Map<String, String> getToProperties() {
        return toProperties;
    }

    public void setToProperties(Map<String, String> toProperties) {
        this.toProperties = toProperties;
    }

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
