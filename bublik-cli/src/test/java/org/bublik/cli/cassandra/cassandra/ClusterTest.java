package org.bublik.cli.cassandra.cassandra;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

import static java.util.Collections.singletonList;

public class ClusterTest {
    private static Network network = Network.newNetwork();
    private static GenericContainer<?> cassandra1 = new GenericContainer<>("cassandra:4.1.10")
            .withEnv("CASSANDRA_SEEDS", "cassandra1,cassandra2,cassandra3")
            .withEnv("CASSANDRA_CLUSTER_NAME", "test")
            .withEnv("CASSANDRA_DC", "DC1")
            .withEnv("CASSANDRA_RACK", "RACK1")
            .withEnv("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
            .withEnv("CASSANDRA_NUM_TOKENS", "128")
            .withExposedPorts(9042)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("cassandra1"))
            .withNetwork(network)
            .withNetworkAliases("cassandra1");
    private static GenericContainer<?> cassandra2 = new GenericContainer<>("cassandra:4.1.10")
            .withEnv("CASSANDRA_SEEDS", "cassandra1,cassandra2,cassandra3")
            .withEnv("CASSANDRA_CLUSTER_NAME", "test")
            .withEnv("CASSANDRA_DC", "DC1")
            .withEnv("CASSANDRA_RACK", "RACK1")
            .withEnv("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
            .withEnv("CASSANDRA_NUM_TOKENS", "128")
            .withExposedPorts(9042)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("cassandra2"))
            .withNetwork(network)
            .withNetworkAliases("cassandra2");
    private static GenericContainer<?> cassandra3 = new GenericContainer<>("cassandra:4.1.10")
            .withEnv("CASSANDRA_SEEDS", "cassandra1,cassandra2,cassandra3")
            .withEnv("CASSANDRA_CLUSTER_NAME", "test")
            .withEnv("CASSANDRA_DC", "DC1")
            .withEnv("CASSANDRA_RACK", "RACK1")
            .withEnv("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
            .withEnv("CASSANDRA_NUM_TOKENS", "128")
            .withExposedPorts(9042)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("cassandra3"))
            .withNetwork(network)
            .withNetworkAliases("cassandra3");

    @BeforeAll
    static void setUp() throws InterruptedException {
        HostPortWaitStrategy csWaitStrategy = new HostPortWaitStrategy();
        csWaitStrategy.forPorts(9042);
        cassandra1.setPortBindings(singletonList("9042:9042"));
        cassandra1.waitingFor(csWaitStrategy);
        cassandra1.start();
        cassandra2.setPortBindings(singletonList("9043:9042"));
        cassandra2.waitingFor(csWaitStrategy);
        cassandra2.start();
        cassandra3.setPortBindings(singletonList("9044:9042"));
        cassandra3.waitingFor(csWaitStrategy);
        cassandra3.start();
    }

    @AfterAll
    static void tearDown() {
        cassandra1.stop();
        cassandra2.stop();
        cassandra3.stop();
        while (cassandra1.isRunning() || cassandra2.isRunning() || cassandra3.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void start() throws InterruptedException {
        Thread.sleep(3_000);
    }

}
