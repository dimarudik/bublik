package org.bublik.cli;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PgToPgSourceSwitchoverTest {
    private static ComposeContainer environment = new ComposeContainer(
            new File("src/test/resources/pg2pg/composev2/docker-compose.yml"))
            .withExposedService("etcd1", 2380)
            .withExposedService("etcd2", 2380)
            .withExposedService("etcd3", 2380)
            .withExposedService("haproxy", 5000)
            .withExposedService("patroni1", 5432)
            .withExposedService("patroni2", 5432)
            .withExposedService("target", 5432);

    @BeforeAll
    static void setUp() throws SQLException {
        environment.start();
        ContainerState patroni1 = getContainer("patroni1");
        ContainerState patroni2 = getContainer("patroni2");
        ContainerState target = getContainer("target");
//        System.out.println(target.getHost() + ":" + target.getContainerId());
    }

    @AfterAll
    static void clear() {
        environment.stop();
    }

//    @Test
    void init() throws IOException {
        assertEquals(0, 0);
    }

    private static ContainerState getContainer(String serviceName) {
        Optional<ContainerState> containerByServiceName = environment.getContainerByServiceName(serviceName);
        return containerByServiceName.orElseThrow();
    }
}
