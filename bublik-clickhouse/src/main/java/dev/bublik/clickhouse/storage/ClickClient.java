package dev.bublik.clickhouse.storage;

import com.clickhouse.client.api.Client;

import java.time.temporal.ChronoUnit;
import java.util.Properties;

public class ClickClient {
    private final int size;
    private final Client client;

    public ClickClient(Properties properties, int size) {
        this.size = size;
        this.client = createClient(properties);
    }

    public Client createClient(Properties properties) {
        return new Client.Builder()
                .addEndpoint(properties.getProperty("url"))
                .setDefaultDatabase(properties.getProperty("database"))
                .setUsername(properties.getProperty("user"))
                .setPassword(properties.getProperty("password"))
                .setMaxConnections(size)
//                .useAsyncRequests(false)
                .setConnectTimeout(10, ChronoUnit.SECONDS)
                .setSocketTimeout(5, ChronoUnit.MINUTES)
                .build();
    }

    public Client getClient() {
        return client;
    }
}
