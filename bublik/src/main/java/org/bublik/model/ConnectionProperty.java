package org.bublik.model;

import java.util.Map;
import java.util.Properties;

public class ConnectionProperty {
    private int threadCount;
    private Boolean rowsStat = false;
    private Map<String, String> fromProperties;
    private Map<String, String> toProperties;
    private Map<String, String> crypto;
    private Map<String, Map<String, String>> toAdds;

    public ConnectionProperty(){}
    public ConnectionProperty(int threadCount,
                              Boolean rowsStat,
                              Map<String, String> fromProperties,
                              Map<String, String> toProperties,
                              Map<String, String> crypto,
                              Map<String, Map<String, String>> toAdds) {
        this.threadCount = threadCount;
        this.rowsStat = rowsStat;
        this.fromProperties = fromProperties;
        this.toProperties = toProperties;
        this.crypto = crypto;
        this.toAdds = toAdds;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public Boolean getRowsStat() {
        return rowsStat;
    }

    public void setRowsStat(Boolean rowsStat) {
        this.rowsStat = rowsStat;
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

    public Properties getCryptoProperties() {
        return getProperties(crypto);
    }

    public Map<String, Map<String, String>> getToAdds() {
        return toAdds;
    }

    public void setToAdds(Map<String, Map<String, String>> toAdds) {
        this.toAdds = toAdds;
    }

    public Map<String, String> getCrypto() {
        return crypto;
    }

    public void setCrypto(Map<String, String> crypto) {
        this.crypto = crypto;
    }

    private Properties getProperties(Map<String, String> map) {
        Properties properties = new Properties();
        properties.putAll(map);
        return properties;
    }
}
