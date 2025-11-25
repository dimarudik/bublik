package org.bublik.cli.cassandra.cassandra;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

//@Disabled
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
    private static Cluster sourceCluster = new Cluster(network, dockerImage,
            List.of(sourceHost1, sourceHost2, sourceHost3), sourcePorts, sourceEnv);
    private static List<GenericContainer<?>> containers = sourceCluster.initCLuster();

    @BeforeAll
    static void setUp() throws InterruptedException {
        HostPortWaitStrategy csWaitStrategy = new HostPortWaitStrategy();
        csWaitStrategy.forPorts(9042);
        final int[] port = {9042};
        containers.forEach(container -> {
            container.setPortBindings(singletonList(port[0] + ":9042"));
            port[0]++;
            container.waitingFor(csWaitStrategy);
            container.start();
        });
    }

    @AfterAll
    static void tearDown() {
        containers.forEach(container -> {
                    try {
                        container.stop();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void start() throws InterruptedException {
        Thread.sleep(3_000);
    }
}
