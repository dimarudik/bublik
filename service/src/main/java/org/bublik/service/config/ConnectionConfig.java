package org.bublik.service.config;

import org.bublik.model.ConnectionProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "bublik")
public class ConnectionConfig {
    private ConnectionProperty connectionProperty;

    public ConnectionProperty getConnectionProperty() {
        return connectionProperty;
    }

    public void setConnectionProperty(ConnectionProperty connectionProperty) {
        this.connectionProperty = connectionProperty;
    }
}
