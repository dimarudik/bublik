package org.example.model;

import lombok.Data;

import java.util.Map;
import java.util.Properties;

@Data
public class Ora2PGProperties {
    private int threadCount;
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
