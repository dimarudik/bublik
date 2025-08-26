package org.bublik.ydb;


import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.bublik.util.ColumnUtil.getCurrentLSN;

// https://medium.com/@kaushikgopu1998/change-data-capture-with-an-example-11a9f73f181d

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
        PGConnection connection = conn.unwrap(PGConnection.class);
        //Drop replication slot
//        connection.getReplicationAPI().dropReplicationSlot("test_slot");
        //Create replication slot
//        createReplicationSlot(connection);

        LogSequenceNumber logSequenceNumber = getCurrentLSN(conn);
        System.out.println(logSequenceNumber.asString());

        PGReplicationStream replicationStream = connection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName("test_slot")
//                .withStartPosition(logSequenceNumber)
                .withSlotOption("include-xids", true) //include the transaction number in BEGIN and COMMIT output
                .withSlotOption("skip-empty-xacts", true) // don't output anything for transactions that didn't modify the database
                .start();

/*
        PGReplicationStream replicationStream =
                connection
                        .getReplicationAPI()
                        .replicationStream()
                        .physical()
                        .withStartPosition(getCurrentLSN(conn))
                        .start();
*/

        while(true) {
            ByteBuffer msg = replicationStream.readPending();
            if(msg == null){
                TimeUnit.MILLISECONDS.sleep(10L);
                continue;
            }
            int offset = msg.arrayOffset();
            byte[] source = msg.array();
            int length = source.length - offset;
            System.out.println(new String(source, offset, length));

            //acknowledgement
            replicationStream.setAppliedLSN(replicationStream.getLastReceiveLSN());
            replicationStream.setFlushedLSN(replicationStream.getLastReceiveLSN());
        }

    }

    private static void createReplicationSlot(PGConnection connection) throws SQLException {
        connection.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName("test_slot")
                .withOutputPlugin("test_decoding")
                .make();
    }
}
