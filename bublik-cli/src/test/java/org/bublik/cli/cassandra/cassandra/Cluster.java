package org.bublik.cli.cassandra.cassandra;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record Cluster (Network network,
                       String dockerImage,
                       List<String> hosts,
                       Integer[] ports,
                       int[] listenPorts,
                       Map<String, String> env
                       ){

    public List<GenericContainer<?>> initCLuster() {
        List<GenericContainer<?>> containers = new ArrayList<>();
        hosts.forEach(host -> {
            GenericContainer<?> container = new GenericContainer<>(dockerImage)
                    .withEnv(env)
                    .withExposedPorts(ports)
                    .waitingFor(Wait.forListeningPorts(listenPorts))
                    .withStartupTimeout(Duration.ofSeconds(180))
                    .withCreateContainerCmdModifier(cmd -> cmd.withHostName(host))
                    .withNetwork(network)
                    .withCommand()
                    .withNetworkAliases(host);
            containers.add(container);
        });
        return containers;
    }
}
