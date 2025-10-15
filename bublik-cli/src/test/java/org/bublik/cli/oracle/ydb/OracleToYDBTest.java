package org.bublik.cli.oracle.ydb;

import org.bublik.cli.App;
import org.bublik.cli.TestResult;
import org.bublik.cli.TestUtils;
import org.bublik.cli.addons.Utils;
import org.bublik.cli.PortsGenerator;
import org.bublik.cli.YdbDockerContainer;
import org.bublik.cli.YdbEnvironment;
import org.bublik.core.model.Config;
import org.bublik.core.model.ConnectionProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.oracle.OracleContainer;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class OracleToYDBTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("oracle/ydb/sql/oracle/ora-init.sql");
    private static YdbDockerContainer target =
            new YdbDockerContainer(new YdbEnvironment(), new PortsGenerator())
                    .withCreateContainerCmdModifier(cmd -> cmd.withHostName("localhost"));

    @BeforeAll
    static void setUp() throws SQLException, InterruptedException {
        source.setPortBindings(java.util.Collections.singletonList("1521:1521"));
        source.start();
        List<String> ports = new ArrayList<>();
        ports.add("2135:2135");
        ports.add("2136:2136");
        ports.add("8765:8765");
        target.setPortBindings(ports);
        target.start();
        while ((!target.isRunning()) && (!source.isRunning())) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        Thread.sleep(10_000);
    }

    @AfterAll
    static void clear() {
        source.stop();
        target.stop();
        while (source.isRunning() || target.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void Foo() throws InterruptedException, IOException {
        Properties sourceProperties = getJdbcProperties(source);
        Properties targetProperties = getJdbcPropertiesOfYdb(target);
        createTargetTable(targetProperties);
        TestResult result = getResult(
                "./oracle/ydb/yaml/ora2ydb.yaml",
                "./oracle/ydb/json/ora2ydb.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);
        assertEquals(result.sourceCount(), result.targetCount());
    }

    private static void createTargetTable(Properties targetProperties) {
        try (Connection connection = DriverManager.getConnection(
                targetProperties.getProperty("url"), targetProperties.getProperty("user"), targetProperties.getProperty("password"))) {
            Statement createTable = connection.createStatement();
            createTable.executeUpdate("create table to_ydb (id Uint32, name String, primary key (id))");
            createTable.close();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static Properties getJdbcPropertiesOfYdb(YdbDockerContainer ydbDockerContainer) {
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:ydb:grpc://");
        sb.append(ydbDockerContainer.getHost());
        sb.append(":2136");
//        sb.append("/");
        sb.append(ydbDockerContainer.database());
        System.out.println("YDB URL: " + sb);
        Properties properties = new Properties();
        properties.setProperty("url", sb.toString());
        properties.setProperty("user", "");
        properties.setProperty("password", "");
        return properties;
    }

    public static TestResult getResult(String connectionPropertyFile,
                                       String mappingFile,
                                       int rows,
                                       boolean sync,
                                       Properties sourceProperties,
                                       Properties targetProperties) throws IOException {
        return getResult(connectionPropertyFile, mappingFile, rows, sync, sourceProperties, targetProperties, null);
    }

    public static TestResult getResult(String connectionPropertyFile,
                                       String mappingFile,
                                       int rows,
                                       boolean sync,
                                       Properties sourceProperties,
                                       Properties targetProperties,
                                       String chunkTableName) throws IOException {
        ConnectionProperty cp = Utils.connectionProperty(TestUtils.getFilePath(connectionPropertyFile));
        List<Config> configs = getConfigs(TestUtils.getFilePath(mappingFile));
        Config config = configs.getFirst();

        App.runProcess(cp, configs, rows, sync, chunkTableName);

        String fromQuery = getQuery(config.fromSchemaName() + "." + config.fromTableName(),
                config.fetchWhereClause() == null ? " 1 = 1 " : config.fetchWhereClause());
        String toQuery = getQuery((config.toTableName() == null ? config.fromTableName() : config.toTableName()),
                " 1 = 1 ");
        Long sourceCount = countRows(sourceProperties, fromQuery);
        Long targetCount = countRows(targetProperties, toQuery);
        return new TestResult(sourceCount, targetCount);
    }

    public static Long countRows(Properties p, String query) {
        try (Connection connection =
                     DriverManager.getConnection(p.getProperty("url"), p.getProperty("user"), p.getProperty("password"))) {
//            System.out.println(p.getProperty("url"));
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            resultSet.next();
            return resultSet.getLong(1);
        }
        catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    private static String getQuery(String tableName, String whereClause) {
        return "SELECT count(1) from " + tableName + " where " + whereClause;
    }

    public static Properties getJdbcProperties(JdbcDatabaseContainer<?> db) {
        Properties properties = new Properties();
        properties.setProperty("url", db.getJdbcUrl());
        properties.setProperty("user", db.getUsername());
        properties.setProperty("password", db.getPassword());
        return properties;
    }
}
