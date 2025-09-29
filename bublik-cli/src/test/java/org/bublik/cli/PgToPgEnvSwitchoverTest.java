package org.bublik.cli;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PgToPgEnvSwitchoverTest {
    private static Network network = Network.newNetwork();
    private static GenericContainer<?> etcd1 = new GenericContainer<>("dimarudik/patroni")
            .withEnv("ETCD_LISTEN_PEER_URLS", "http://0.0.0.0:2380")
            .withEnv("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379")
            .withEnv("ETCD_INITIAL_CLUSTER", "etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380")
            .withEnv("ETCD_INITIAL_CLUSTER_STATE", "new")
            .withEnv("ETCD_INITIAL_CLUSTER_TOKEN", "tutorial")
            .withEnv("ETCD_UNSUPPORTED_ARCH", "arm64")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("etcd1"))
            .withCommand("etcd --name etcd1 --initial-advertise-peer-urls http://etcd1:2380")
            .withNetwork(network);

    private static GenericContainer<?> etcd2 = new GenericContainer<>("dimarudik/patroni")
            .withEnv("ETCD_LISTEN_PEER_URLS", "http://0.0.0.0:2380")
            .withEnv("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379")
            .withEnv("ETCD_INITIAL_CLUSTER", "etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380")
            .withEnv("ETCD_INITIAL_CLUSTER_STATE", "new")
            .withEnv("ETCD_INITIAL_CLUSTER_TOKEN", "tutorial")
            .withEnv("ETCD_UNSUPPORTED_ARCH", "arm64")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("etcd2"))
            .withCommand("etcd --name etcd2 --initial-advertise-peer-urls http://etcd2:2380")
            .withNetwork(network);

    private static GenericContainer<?> etcd3 = new GenericContainer<>("dimarudik/patroni")
            .withEnv("ETCD_LISTEN_PEER_URLS", "http://0.0.0.0:2380")
            .withEnv("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379")
            .withEnv("ETCD_INITIAL_CLUSTER", "etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380")
            .withEnv("ETCD_INITIAL_CLUSTER_STATE", "new")
            .withEnv("ETCD_INITIAL_CLUSTER_TOKEN", "tutorial")
            .withEnv("ETCD_UNSUPPORTED_ARCH", "arm64")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("etcd3"))
            .withCommand("etcd --name etcd3 --initial-advertise-peer-urls http://etcd3:2380")
            .withNetwork(network);

    @BeforeAll
    static void setUp() {
        etcd1.start();
        etcd2.start();
        etcd3.start();
    }

    @Test
    public void init() throws InterruptedException {
        Thread.sleep(120_000);
        assertEquals(0, 0);
    }
}
