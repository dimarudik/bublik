package org.bublik.cli;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PgToPgEnvSwitchoverTest {
//    private static Network network = Network.newNetwork();
    private static GenericContainer<?> etcd1 = new GenericContainer<>("dimarudik/patroni")
            .withEnv("ETCD_LISTEN_PEER_URLS", "http://0.0.0.0:2380")
            .withEnv("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379")
            .withEnv("ETCD_INITIAL_CLUSTER", "etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380")
            .withEnv("ETCD_INITIAL_CLUSTER_STATE", "new")
            .withEnv("ETCD_INITIAL_CLUSTER_TOKEN", "tutorial")
            .withEnv("ETCD_UNSUPPORTED_ARCH", "arm64")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("etcd1"))
            .withCommand("etcd --name etcd1 --initial-advertise-peer-urls http://etcd1:2380");
//            .withNetwork(network);

    private static GenericContainer<?> etcd2 = new GenericContainer<>("dimarudik/patroni")
            .withEnv("ETCD_LISTEN_PEER_URLS", "http://0.0.0.0:2380")
            .withEnv("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379")
            .withEnv("ETCD_INITIAL_CLUSTER", "etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380")
            .withEnv("ETCD_INITIAL_CLUSTER_STATE", "new")
            .withEnv("ETCD_INITIAL_CLUSTER_TOKEN", "tutorial")
            .withEnv("ETCD_UNSUPPORTED_ARCH", "arm64")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("etcd2"))
            .withCommand("etcd --name etcd2 --initial-advertise-peer-urls http://etcd2:2380");
//            .withNetwork(network);

    private static GenericContainer<?> etcd3 = new GenericContainer<>("dimarudik/patroni")
            .withEnv("ETCD_LISTEN_PEER_URLS", "http://0.0.0.0:2380")
            .withEnv("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379")
            .withEnv("ETCD_INITIAL_CLUSTER", "etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380")
            .withEnv("ETCD_INITIAL_CLUSTER_STATE", "new")
            .withEnv("ETCD_INITIAL_CLUSTER_TOKEN", "tutorial")
            .withEnv("ETCD_UNSUPPORTED_ARCH", "arm64")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("etcd3"))
            .withCommand("etcd --name etcd3 --initial-advertise-peer-urls http://etcd3:2380");
//            .withNetwork(network);

    private static GenericContainer<?> patroni1 = new GenericContainer<>("dimarudik/patroni")
            .withEnv("PATRONI_RESTAPI_USERNAME", "admin")
            .withEnv("PATRONI_RESTAPI_PASSWORD", "admin")
            .withEnv("PATRONI_SUPERUSER_USERNAME", "postgres")
            .withEnv("PATRONI_SUPERUSER_PASSWORD", "postgres")
            .withEnv("PATRONI_REPLICATION_USERNAME", "replicator")
            .withEnv("PATRONI_REPLICATION_PASSWORD", "replicate")
            .withEnv("PATRONI_admin_PASSWORD", "admin")
            .withEnv("PATRONI_admin_OPTIONS", "createdb,createrole")
            .withEnv("ETCD_ENDPOINTS", "http://etcd1:2379,http://etcd2:2379,http://etcd3:2379")
            .withEnv("PATRONI_ETCD3_HOSTS", "'etcd1:2379','etcd2:2379','etcd3:2379'")
            .withEnv("PATRONI_SCOPE", "demo")
            .withEnv("PATRONI_NAME", "patroni1")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("patroni1"));
//            .withNetwork(network);

    private static GenericContainer<?> patroni2 = new GenericContainer<>("dimarudik/patroni")
            .withEnv("PATRONI_RESTAPI_USERNAME", "admin")
            .withEnv("PATRONI_RESTAPI_PASSWORD", "admin")
            .withEnv("PATRONI_SUPERUSER_USERNAME", "postgres")
            .withEnv("PATRONI_SUPERUSER_PASSWORD", "postgres")
            .withEnv("PATRONI_REPLICATION_USERNAME", "replicator")
            .withEnv("PATRONI_REPLICATION_PASSWORD", "replicate")
            .withEnv("PATRONI_admin_PASSWORD", "admin")
            .withEnv("PATRONI_admin_OPTIONS", "createdb,createrole")
            .withEnv("ETCD_ENDPOINTS", "http://etcd1:2379,http://etcd2:2379,http://etcd3:2379")
            .withEnv("PATRONI_ETCD3_HOSTS", "'etcd1:2379','etcd2:2379','etcd3:2379'")
            .withEnv("PATRONI_SCOPE", "demo")
            .withEnv("PATRONI_NAME", "patroni2")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("patroni2"));
//            .withNetwork(network);

    private static GenericContainer<?> target = new GenericContainer<>("postgres")
            .withEnv("POSTGRES_USER", "postgres")
            .withEnv("POSTGRES_PASSWORD", "postgres")
            .withEnv("POSTGRES_DB", "postgres")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("target"));
//            .withNetwork(network);

    @BeforeAll
    static void setUp() {
        etcd1.start();
        etcd2.start();
        etcd3.start();
        patroni1.setPortBindings(java.util.Collections.singletonList("5432:5432"));
        patroni2.setPortBindings(java.util.Collections.singletonList("5433:5432"));
        target.setPortBindings(java.util.Collections.singletonList("5434:5432"));
        patroni1.start();
        patroni2.start();
        target.start();
    }

    @Test
    public void init() throws InterruptedException {
        Thread.sleep(120_000);
        assertEquals(0, 0);
    }
}
