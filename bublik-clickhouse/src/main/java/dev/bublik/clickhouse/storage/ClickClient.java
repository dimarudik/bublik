package dev.bublik.clickhouse.storage;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.query.QueryResponse;

import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ClickClient {
    private final int size;
    private final Client client;

    public ClickClient(Client client) {
        this.client = client;
        this.size = Integer.parseInt(client.getConfiguration().get(ClientConfigProperties.HTTP_MAX_OPEN_CONNECTIONS.getKey()));
    }

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
                .setConnectTimeout(10, ChronoUnit.SECONDS)
                .setSocketTimeout(5, ChronoUnit.MINUTES)
                .build();
    }

    public Client getClient() {
        return client;
    }

    public int getSize() {
        return size;
    }

    public void ping() {
        try (QueryResponse response = client.query("SELECT 1").get(10, TimeUnit.SECONDS)) {
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to ClickHouse server", e);
        }
    }
}
