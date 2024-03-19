package org.example.model;

import java.util.Map;
import java.util.Properties;

//@Data
public class Ora2PGProperties {
    private int threadCount;
    private Boolean initPGChunks;
    private Boolean copyPGChunks;
    private Map<String, String> fromProperties;
    private Map<String, String> toProperties;

    public Boolean getCopyPGChunks() {
        return copyPGChunks;
    }

    public void setCopyPGChunks(Boolean copyPGChunks) {
        this.copyPGChunks = copyPGChunks;
    }

    public Boolean getInitPGChunks() {
        return initPGChunks;
    }

    public void setInitPGChunks(Boolean initPGChunks) {
        this.initPGChunks = initPGChunks;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
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
