package dev.bublik.cli.oracle.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import dev.bublik.cli.App;
import dev.bublik.cli.TestUtils;
import dev.bublik.cli.addons.Utils;
import org.bublik.core.model.Config;
import org.bublik.core.model.ConnectionProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.oracle.OracleContainer;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static dev.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.assertTrue;

//@Disabled
public class ToUserDefinedTypeTest {
    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("./oracle/cassandra/sql/oracle/ora-init.sql");
    private static CassandraContainer target = new CassandraContainer("cassandra")
            .withInitScript("./oracle/cassandra/sql/cs-to-udt.cql");

    @BeforeAll
    static void setUp() throws SQLException {
        source.setPortBindings(java.util.Collections.singletonList("1521:1521"));
        source.start();
        target.setPortBindings(java.util.Collections.singletonList("9042:9042"));
        target.start();
        while (!source.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
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
    public void toUDT() throws InterruptedException, IOException {
        Properties sourceProperties = getJdbcProperties(source);
        Properties targetProperties = getJdbcPropertiesOfCassandra(target);
        boolean result = getResult(
                "./oracle/cassandra/yaml/ora2cs-to-list.yaml",
                "./oracle/cassandra/json/to-udt.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);
        assertTrue(result);
//        Thread.sleep(180_000);
    }

    public static boolean getResult(String connectionPropertyFile,
                                    String mappingFile,
                                    int rows,
                                    boolean sync,
                                    Properties sourceProperties,
                                    Properties targetProperties) throws IOException {
        ConnectionProperty cp = Utils.connectionProperty(TestUtils.getFilePath(connectionPropertyFile));
        List<Config> configs = getConfigs(TestUtils.getFilePath(mappingFile));

        App.runProcess(cp, configs, rows, sync);

        return resultCassandra();
    }

    private static boolean resultCassandra() {
        int pageSize = 10000;
        CqlSession cqlSession = CqlSession
                .builder()
                .addContactPoint(target.getContactPoint())
                .withLocalDatacenter(target.getLocalDatacenter())
                .build();
        SimpleStatement stmt1 = SimpleStatement.builder("SELECT * FROM test.to_list")
                .setPageSize(pageSize) // set page size
                .build();
        com.datastax.oss.driver.api.core.cql.ResultSet resultSet = cqlSession.execute(stmt1);
        UserDefinedType udt = cqlSession
                .getMetadata()
                .getKeyspace("test")
                .get()
                .getUserDefinedType("complex_body")
                .get();
        for (com.datastax.oss.driver.api.core.cql.Row row : resultSet) {
            if (!(row.getInt("id") == 1)) {
                return false;
            }
            UdtValue udtValue = row.getUdtValue("body");
            if(udtValue == null || !(udtValue.getInt("parent_id") == 2)) {
                System.out.println(udtValue.getInt("parent_id"));
                return false;
            }
            if(!udtValue.getString("user_name").equals("user1user1@gmail.com")) {
                return false;
            }
            if(!udtValue.getString("email").equals("user1@gmail.com")) {
                return false;
            }
/*
            if(!udtValue.getInstant("last_update").toString().equals("2024-12-31T21:00:00Z")) {
                return false;
            }
*/
        }
        cqlSession.close();
        return true;
    }

    private static Properties getJdbcProperties(JdbcDatabaseContainer<?> db) {
        Properties properties = new Properties();
        properties.setProperty("url", db.getJdbcUrl());
        properties.setProperty("user", db.getUsername());
        properties.setProperty("password", db.getPassword());
        return properties;
    }

    private Properties getJdbcPropertiesOfCassandra(CassandraContainer target) {
        Properties properties = new Properties();
        properties.setProperty("class", "org.bublik.cassandra.storage.CSPoolStorage");
        properties.setProperty("keyspace", "test");
        properties.setProperty("user", "test");
        properties.setProperty("password", "test");
        properties.setProperty("datacenter", "datacenter1");
        properties.setProperty("batchSize", "256");
        return properties;
    }
}
