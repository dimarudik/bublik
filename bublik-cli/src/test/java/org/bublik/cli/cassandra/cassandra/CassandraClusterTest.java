package org.bublik.cli.cassandra.cassandra;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.delegate.CassandraDatabaseDelegate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

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
    private static final int[] listenPorts = {9042};
    private static final Integer[] ports = {7000, 7199, 9042};
    private static final Map<String, String> sourceEnv = envMap("source", "DC1", "RACK1",
            sourceHost1, sourceHost2, sourceHost3);
    private static final Map<String, String> targetEnv = envMap("target", "DC2", "RACK2",
            targetHost1, targetHost2, targetHost3);
    private static final Cluster sourceCluster = new Cluster(network, dockerImage,
            List.of(sourceHost1, sourceHost2, sourceHost3), ports, listenPorts, sourceEnv);
    private static final Cluster targetCluster = new Cluster(network, dockerImage,
            List.of(targetHost1, targetHost2, targetHost3), ports, listenPorts, targetEnv);
    private static final List<GenericContainer<?>> sourceContainers = sourceCluster.initCLuster();
    private static final List<GenericContainer<?>> targetContainers = targetCluster.initCLuster();

    @BeforeAll
    static void setUp() throws InterruptedException {
        MountableFile sourceInit = MountableFile.forClasspathResource("./cassandra/cassandra/sql/cs-init-rf3.cql");
        MountableFile targetInit = MountableFile.forClasspathResource("./cassandra/cassandra/sql/cs-init-empty-rf3.cql");
        GenericContainer<?> sourceLeader = sourceContainers.getFirst();
        GenericContainer<?> targetLeader = targetContainers.getFirst();
        final int[] port = {9042};
        sourceContainers.forEach(container -> {
            container.setPortBindings(singletonList(port[0] + ":9042"));
            port[0]++;
            container.start();
        });
        do {
            boolean allRunning = sourceContainers.stream().allMatch(Container::isRunning);
            if (allRunning) {
                sourceLeader.copyFileToContainer(sourceInit, "/init.cql");
                (new CassandraDatabaseDelegate(sourceLeader)).execute(null, "/init.cql", -1, false, false);
                break;
            } else {
                Thread.sleep(200);
            }
        } while (true);
        targetContainers.forEach(container -> {
            container.setPortBindings(singletonList(port[0] + ":9042"));
            port[0]++;
            container.start();
        });
        do {
            boolean allRunning = targetContainers.stream().allMatch(Container::isRunning);
            if (allRunning) {
                targetLeader.copyFileToContainer(targetInit, "/init.cql");
                (new CassandraDatabaseDelegate(targetLeader)).execute(null, "/init.cql", -1, false, false);
                break;
            } else {
                Thread.sleep(200);
            }
        } while (true);
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
//        Thread.sleep(300_000);
    }

    public static Map<String, String> envMap(String clusterName, String dataCenter, String rack, String... hosts) {
        Map<String, String> srcEnv = new HashMap<>();
        String seeds = String.join(",", hosts);
        srcEnv.put("JVM_OPTS", "-Xms384M -Xmx384M");
        srcEnv.put("CASSANDRA_SEEDS", seeds);
        srcEnv.put("CASSANDRA_CLUSTER_NAME", clusterName);
        srcEnv.put("CASSANDRA_DC", dataCenter);
        srcEnv.put("CASSANDRA_RACK", rack);
        srcEnv.put("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch");
        srcEnv.put("CASSANDRA_NUM_TOKENS", "128");
        return srcEnv;
    }
}
