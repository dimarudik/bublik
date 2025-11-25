package org.bublik.cli.cassandra.cassandra;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;

@Disabled
public class CassandraClusterTest {
    private static Network network = Network.newNetwork();
    private static String dockerImage = "cassandra:4.1.10";
    private static String sourceHost1 = "cassandra1";
    private static String sourceHost2 = "cassandra2";
    private static String sourceHost3 = "cassandra3";
    private static Integer[] sourcePorts = {7000, 7199, 9042};
    private static Map<String, String> sourceEnv = new HashMap<>();
    static {
        sourceEnv.put("CASSANDRA_SEEDS", "cassandra1,cassandra2,cassandra3");
        sourceEnv.put("CASSANDRA_CLUSTER_NAME", "test");
        sourceEnv.put("CASSANDRA_DC", "DC1");
        sourceEnv.put("CASSANDRA_RACK", "RACK1");
        sourceEnv.put("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch");
        sourceEnv.put("CASSANDRA_NUM_TOKENS", "128");
    }

    private static GenericContainer<?> sourceNode1 = new GenericContainer<>(dockerImage)
            .withEnv(sourceEnv)
            .withExposedPorts(sourcePorts)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName(sourceHost1))
            .withNetwork(network)
            .withNetworkAliases(sourceHost1);
    private static GenericContainer<?> sourceNode2 = new GenericContainer<>(dockerImage)
            .withEnv(sourceEnv)
            .withExposedPorts(sourcePorts)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName(sourceHost2))
            .withNetwork(network)
            .withNetworkAliases(sourceHost2);
    private static GenericContainer<?> sourceNode3 = new GenericContainer<>(dockerImage)
            .withEnv(sourceEnv)
            .withExposedPorts(sourcePorts)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName(sourceHost3))
            .withNetwork(network)
            .withNetworkAliases(sourceHost3);

    @BeforeAll
    static void setUp() throws InterruptedException {
        HostPortWaitStrategy csWaitStrategy = new HostPortWaitStrategy();
        csWaitStrategy.forPorts(9042);
        sourceNode1.setPortBindings(singletonList("9042:9042"));
        sourceNode1.waitingFor(csWaitStrategy);
        sourceNode1.start();
        sourceNode2.setPortBindings(singletonList("9043:9042"));
        sourceNode2.waitingFor(csWaitStrategy);
        sourceNode2.start();
        sourceNode3.setPortBindings(singletonList("9044:9042"));
        sourceNode3.waitingFor(csWaitStrategy);
        sourceNode3.start();
    }

    @AfterAll
    static void tearDown() {
        sourceNode1.stop();
        sourceNode2.stop();
        sourceNode3.stop();
        while (sourceNode1.isRunning() || sourceNode2.isRunning() || sourceNode3.isRunning()) {
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
