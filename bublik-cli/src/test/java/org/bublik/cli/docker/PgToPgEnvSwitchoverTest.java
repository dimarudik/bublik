package org.bublik.cli.docker;

import io.restassured.RestAssured;
import io.restassured.config.HttpClientConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import org.apache.http.params.CoreConnectionPNames;
import org.bublik.cli.TestResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.restassured.RestAssured.given;
import static org.bublik.cli.TestUtils.getJdbcProperties;
import static org.bublik.cli.TestUtils.getResult;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class PgToPgEnvSwitchoverTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static String etcdHostName1 = "etcd1";
    private static String etcdHostName2 = "etcd2";
    private static String etcdHostName3 = "etcd3";
    private static String patroni1PortBinding1 = "5432:5432";
    private static String patroni1PortBinding2 = "8008:8008";
    private static String patroni2PortBinding1 = "5433:5432";
    private static String targetPortBinding1 = "5434:5432";
    private static Network network = Network.newNetwork();
    private static GenericContainer<?> etcd1 = new GenericContainer<>("dimarudik/patroni")
            .withEnv("ETCD_LISTEN_PEER_URLS", "http://0.0.0.0:2380")
            .withEnv("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379")
            .withEnv("ETCD_INITIAL_CLUSTER", "etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380")
            .withEnv("ETCD_INITIAL_CLUSTER_STATE", "new")
            .withEnv("ETCD_INITIAL_CLUSTER_TOKEN", "tutorial")
            .withEnv("ETCD_UNSUPPORTED_ARCH", "arm64")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName(etcdHostName1))
            .withNetwork(network)
            .withCommand("etcd --name " + etcdHostName1 + " --initial-advertise-peer-urls http://" + etcdHostName1 +":2380");

    private static GenericContainer<?> etcd2 = new GenericContainer<>("dimarudik/patroni")
            .withEnv("ETCD_LISTEN_PEER_URLS", "http://0.0.0.0:2380")
            .withEnv("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379")
            .withEnv("ETCD_INITIAL_CLUSTER", "etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380")
            .withEnv("ETCD_INITIAL_CLUSTER_STATE", "new")
            .withEnv("ETCD_INITIAL_CLUSTER_TOKEN", "tutorial")
            .withEnv("ETCD_UNSUPPORTED_ARCH", "arm64")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName(etcdHostName2))
            .withNetwork(network)
            .withCommand("etcd --name " + etcdHostName2 + " --initial-advertise-peer-urls http://" + etcdHostName2 +":2380");

    private static GenericContainer<?> etcd3 = new GenericContainer<>("dimarudik/patroni")
            .withEnv("ETCD_LISTEN_PEER_URLS", "http://0.0.0.0:2380")
            .withEnv("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379")
            .withEnv("ETCD_INITIAL_CLUSTER", "etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380")
            .withEnv("ETCD_INITIAL_CLUSTER_STATE", "new")
            .withEnv("ETCD_INITIAL_CLUSTER_TOKEN", "tutorial")
            .withEnv("ETCD_UNSUPPORTED_ARCH", "arm64")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName(etcdHostName3))
            .withNetwork(network)
            .withCommand("etcd --name " + etcdHostName3 + " --initial-advertise-peer-urls http://" + etcdHostName3 +":2380");

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
            .withNetwork(network)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("patroni1"));

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
            .withNetwork(network)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("patroni2"));

    private static JdbcDatabaseContainer<?> target = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withNetwork(network)
            .withInitScript("./pg2pg/composev2/sql/init-target.sql")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("target"));

    @BeforeAll
    static void setUp() throws InterruptedException {
        etcd1.setPortBindings(java.util.Collections.singletonList("2379:2379"));
        etcd1.start();
        etcd2.start();
        etcd3.start();
        List<String> ports = new ArrayList<>();
        ports.add(patroni1PortBinding1);
        ports.add(patroni1PortBinding2);
        patroni1.setPortBindings(ports);
        patroni2.setPortBindings(java.util.Collections.singletonList(patroni2PortBinding1));
        target.setPortBindings(java.util.Collections.singletonList(targetPortBinding1));
        patroni1.start();
        patroni2.start();
        target.start();
        Thread.sleep(10_000);
        setSynchronousMode();
        Thread.sleep(10_000);
/*
        while (!patroni1.isRunning() && !patroni2.isRunning() && !target.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
*/
    }

    @AfterAll
    static void tearDown() {
        patroni1.stop();
        patroni2.stop();
        target.stop();
        etcd1.stop();
        etcd2.stop();
        etcd3.stop();
        while (patroni1.isRunning() || patroni2.isRunning() || target.isRunning() || etcd1.isRunning() || etcd2.isRunning() || etcd3.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void etcdReadiness() throws InterruptedException, URISyntaxException {
        RestAssured.baseURI = "http://" + etcd1.getHost() + ":2379";
        RestAssuredConfig config = RestAssured.config()
                .httpClient(HttpClientConfig.httpClientConfig()
                        .setParam(CoreConnectionPNames.CONNECTION_TIMEOUT, 3000)
                        .setParam(CoreConnectionPNames.SO_TIMEOUT, 3000));
        given()
                .when()
                .config(config)
                .get("/v2/members")
                .then()
                .assertThat()
                .statusCode(200)
                .body("members", hasSize(3));
    }

    @Test
    // curl http://localhost:8008/cluster | jq .
    public void patroniReadiness() throws InterruptedException, URISyntaxException {
        RestAssured.baseURI = "http://" + patroni1.getHost() + ":8008";
        RestAssuredConfig config = RestAssured.config()
                .httpClient(HttpClientConfig.httpClientConfig()
                        .setParam(CoreConnectionPNames.CONNECTION_TIMEOUT, 3000)
                        .setParam(CoreConnectionPNames.SO_TIMEOUT, 3000));
        given()
                .when()
                .config(config)
                .get("/cluster")
                .then()
                .assertThat()
                .statusCode(200)
                .body("members", hasSize(2));
    }

    @Test
    public void switchoverTest() throws IOException, InterruptedException, ExecutionException {
        Properties sourceProperties = getJdbcPropertiesOfGeneric(patroni1, patroni2);
        Properties targetProperties = getJdbcProperties(target);
        prepareSource(sourceProperties);
        CompletableFuture<TestResult> future = CompletableFuture.supplyAsync(() -> {
            try {
                return getResult(
                        "pg2pg/composev2/pg2pgSwitchover.yaml",
                        "pg2pg/composev2/pg2pgswitchover.json",
                        rows,
                        sync,
                        sourceProperties,
                        targetProperties);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        switchover();

        TestResult result = future.get();
        assertEquals(result.targetCount(), result.sourceCount());
    }

// curl -s http://localhost:8008/leader | jq .
// curl --user admin:admin -s http://localhost:8008/switchover -POST -d '{"leader":"patroni1"}'
    @Test
    public void switchover() throws InterruptedException {
        Thread.sleep(5_000);
        RestAssured.baseURI = "http://" + patroni1.getHost() + ":8008";
        RestAssuredConfig config = RestAssured.config()
                .httpClient(HttpClientConfig.httpClientConfig()
                        .setParam(CoreConnectionPNames.CONNECTION_TIMEOUT, 3000)
                        .setParam(CoreConnectionPNames.SO_TIMEOUT, 3000));
        Response response =
                given()
                        .when()
                        .config(config)
                        .get("/leader");
        String leader = response.jsonPath().getString("role").equals("primary") ? "patroni1" : "patroni2";

        given()
                .auth().basic("admin", "admin")
                .config(config)
                .contentType(ContentType.JSON)
                .body("{\"leader\":\"" + leader + "\"}")
                .when()
                .post("/switchover")
                .then()
                .log().body()
                .assertThat()
                .statusCode(200);
    }

    private static Properties getJdbcPropertiesOfGeneric(GenericContainer<?>... dbs) {
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:postgresql://");
        Arrays.stream(dbs).forEach(db ->
                sb.append(db.getHost())
                        .append(":")
                        .append(db.getPortBindings().stream().filter(port -> port.contains("5432")).findFirst().get().split("/")[0].split(":")[0])
                        .append(",")
        );
        sb.deleteCharAt(sb.length() - 1);
        sb.append("/postgres?targetServerType=primary&options=-c%20enable_indexscan=off%20-c%20enable_indexonlyscan=off%20-c%20enable_bitmapscan=off");
        Properties properties = new Properties();
        properties.setProperty("url", sb.toString());
        properties.setProperty("user", dbs[0].getEnvMap().get("PATRONI_SUPERUSER_USERNAME"));
        properties.setProperty("password", dbs[0].getEnvMap().get("PATRONI_SUPERUSER_PASSWORD"));
        return properties;
    }

    private void prepareSource(Properties p) {
        try (Connection connection =
                     DriverManager.getConnection(p.getProperty("url"), p.getProperty("user"), p.getProperty("password"))) {
            Statement createTable = connection.createStatement();
            createTable.executeUpdate("create table public.switchover (id bigint, name varchar(512))");
            createTable.close();
            Statement insertData = connection.createStatement();
            insertData.executeUpdate(
                    "insert into public.switchover (id, name) " +
                    "select num as id, 'Name ' || substr(md5(random()::text), 1, 512) as name " +
                    "from generate_series(1, 19000000) as num"
            );
            insertData.close();
            Statement analyzeTable = connection.createStatement();
            analyzeTable.executeUpdate("analyze public.switchover");
            analyzeTable.close();
            System.out.println("Source prepared");
        }
        catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    // curl -s http://localhost:8008/config | jq .
    // curl --user admin:admin -s -XPATCH -d '{"synchronous_mode": true}' http://localhost:8008/config | jq .
    // synchronous_mode: true
    private static void setSynchronousMode() {
        RestAssured.baseURI = "http://" + patroni1.getHost() + ":8008";
        RestAssuredConfig config = RestAssured.config()
                .httpClient(HttpClientConfig.httpClientConfig()
                        .setParam(CoreConnectionPNames.CONNECTION_TIMEOUT, 3000)
                        .setParam(CoreConnectionPNames.SO_TIMEOUT, 3000));
        given()
                .auth().basic("admin", "admin")
                .config(config)
                .contentType(ContentType.JSON)
                .body("{\"synchronous_mode\": true}")
                .when()
                .patch("/config");
    }
}
