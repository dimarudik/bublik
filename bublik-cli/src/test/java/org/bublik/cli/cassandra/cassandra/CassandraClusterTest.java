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
    private static final Network network = Network.newNetwork();
    private static final String dockerImage = "cassandra:4.1.10";
    private static final String sourceHost1 = "source1";
    private static final String sourceHost2 = "source2";
    private static final String sourceHost3 = "source3";
    private static final String targetHost1 = "target1";
    private static final String targetHost2 = "target2";
    private static final String targetHost3 = "target3";
    private static final Integer[] ports = {7000, 7199, 9042};
    private static final Map<String, String> sourceEnv = envMap(sourceHost1, sourceHost2, sourceHost3);
    private static final Map<String, String> targetEnv = envMap(targetHost1, targetHost2, targetHost3);
    private static final Cluster sourceCluster = new Cluster(network, dockerImage,
            List.of(sourceHost1, sourceHost2, sourceHost3), ports, sourceEnv);
    private static final Cluster targetCluster = new Cluster(network, dockerImage,
            List.of(targetHost1, targetHost2, targetHost3), ports, targetEnv);
    private static final List<GenericContainer<?>> sourceContainers = sourceCluster.initCLuster();
    private static final List<GenericContainer<?>> targetContainers = targetCluster.initCLuster();

    @BeforeAll
    static void setUp() throws InterruptedException {
//        HostPortWaitStrategy sourceStrategy = new HostPortWaitStrategy();
//        sourceStrategy.forPorts(9042);
//        HostPortWaitStrategy targetStrategy = new HostPortWaitStrategy();
//        targetStrategy.forPorts(9042);
        final int[] port = {9042};
        sourceContainers.forEach(container -> {
            container.setPortBindings(singletonList(port[0] + ":9042"));
            port[0]++;
//            container.waitingFor(sourceStrategy);
            container.start();
        });
/*
        targetContainers.forEach(container -> {
            container.setPortBindings(singletonList(port[0] + ":9042"));
            port[0]++;
            container.waitingFor(targetStrategy);
            container.start();
        });
*/

    }

    @AfterAll
    static void tearDown() {
        sourceContainers.forEach(container -> {
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

    public static Map<String, String> envMap(String... hosts) {
        Map<String, String> srcEnv = new HashMap<>();
        String seeds = String.join(",", hosts);
        srcEnv.put("CASSANDRA_SEEDS", seeds);
        srcEnv.put("CASSANDRA_CLUSTER_NAME", "test");
        srcEnv.put("CASSANDRA_DC", "DC1");
        srcEnv.put("CASSANDRA_RACK", "RACK1");
        srcEnv.put("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch");
        srcEnv.put("CASSANDRA_NUM_TOKENS", "128");
        return srcEnv;
    }
}
