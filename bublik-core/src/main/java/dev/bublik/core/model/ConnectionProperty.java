package dev.bublik.core.model;

import java.util.Map;
import java.util.Properties;

public class ConnectionProperty {
    private int threadCount;
    private Map<String, String> fromProperties;
    private Map<String, String> toProperties;

    public ConnectionProperty(){}
    public ConnectionProperty(int threadCount,
                              Map<String, String> fromProperties,
                              Map<String, String> toProperties) {
        this.threadCount = threadCount;
        this.fromProperties = fromProperties;
        this.toProperties = toProperties;
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
