package org.bublik.ydb;


import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.util.PSQLException;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Sync {
    public static void main(String[] args) throws SQLException, InterruptedException {

        String url = "jdbc:postgresql://localhost:5432/postgres";
        Properties props = new Properties();
        PGProperty.USER.set(props, "test");
        PGProperty.PASSWORD.set(props, "test");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        Connection conn = DriverManager.getConnection(url, props);
        PGConnection replConnection = conn.unwrap(PGConnection.class);
        //Drop replication slot
/*
        try {
            replConnection.getReplicationAPI().dropReplicationSlot("test_slot");
        } catch (PSQLException e) {
            System.out.println(e);
        }
*/
        //Create replication slot
        createReplicationSlot(replConnection);
        //Create stream
        PGReplicationStream stream = replConnection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName("test_slot")
                .withSlotOption("include-xids", false) //include the transaction number in BEGIN and COMMIT output
                .withSlotOption("skip-empty-xacts", true) // don't output anything for transactions that didn't modify the database
                .start();

        while(true) {
            ByteBuffer msg = stream.readPending();
            if(msg == null){
                TimeUnit.MILLISECONDS.sleep(10L);
                continue;
            }
            int offset = msg.arrayOffset();
            byte[] source = msg.array();
            int length = source.length - offset;
            System.out.println(new String(source, offset, length));

            //acknowledgement
            stream.setAppliedLSN(stream.getLastReceiveLSN());
            stream.setFlushedLSN(stream.getLastReceiveLSN());
        }
    }

    private static void createReplicationSlot(PGConnection replConnection) throws SQLException {
        replConnection.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName("test_slot")
                .withOutputPlugin("test_decoding")
                .make();
    }
}
