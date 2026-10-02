package dev.bublik.core.model;

import java.util.HashMap;
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int threadCount;
        private Map<String, String> fromProperties = new HashMap<>();
        private Map<String, String> toProperties = new HashMap<>();

        public Builder threadCount(int threadCount) {
            this.threadCount = threadCount;
            return this;
        }

        public Builder fromProperties(Map<String, String> fromProperties) {
            if (fromProperties != null) {
                this.fromProperties = new HashMap<>(fromProperties);
            }
            return this;
        }

        public Builder toProperties(Map<String, String> toProperties) {
            if (toProperties != null) {
                this.toProperties = new HashMap<>(toProperties);
            }
            return this;
        }

        public Builder addFromProperty(String key, String value) {
            this.fromProperties.put(key, value);
            return this;
        }

        public Builder addToProperty(String key, String value) {
            this.toProperties.put(key, value);
            return this;
        }

        public ConnectionProperty build() {
            return new ConnectionProperty(threadCount, fromProperties, toProperties);
        }
    }
}
