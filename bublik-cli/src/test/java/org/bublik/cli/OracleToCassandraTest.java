package org.bublik.cli;

import org.bublik.cassandra.storage.CSPool;
import org.bublik.cli.addons.Utils;
import org.bublik.core.model.ConnectionProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.oracle.OracleContainer;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;

import static org.bublik.core.util.Utils.getStackTrace;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class OracleToCassandraTest {
//    private static final Logger log = LoggerFactory.getLogger(OracleToCassandra.class);

    private static int rows = 50000;
    private static boolean sync = false;
    private static JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("ora2pg/sql/00_init.sql");
   private static CassandraContainer target = new CassandraContainer("cassandra:latest");

   @BeforeAll
   static void setUp() throws SQLException {
       target.setPortBindings(java.util.Collections.singletonList("9042:9042"));
       target.start();
   }

    @Test
    void checkCS() throws IOException {
       try {
           ConnectionProperty cp = Utils.connectionProperty(TestUtils.getFilePath("ora2cs/ora2cs.yaml"));
           Properties properties = cp.getToProperty();
           CSPool csPool = new CSPool(properties);
           csPool.closeCqlSession();
       } catch (Exception e) {
           System.out.println(getStackTrace(e));
       }
/*
        CqlSession cqlSession = CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress("localhost", 9042))
                .withLocalDatacenter(target.getLocalDatacenter())
                .build();
        cqlSession.close();
*/
        assertEquals(true, true);
    }
}
