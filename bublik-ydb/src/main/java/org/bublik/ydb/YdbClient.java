package org.bublik.ydb;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.bublik.exception.Utils.getStackTrace;

public class YdbClient {
    private static final Logger log = LoggerFactory.getLogger(YdbClient.class);

    private final GrpcTransport transport;
    private final TableClient tableClient;

    public YdbClient(String connectionString, String certFile) {
        StaticCredentials authProvider = new StaticCredentials("root", "passw0rd");
//        StaticCredentials authProvider = new StaticCredentials("", "");

        byte[] cert;
        try {
            cert = Files.readAllBytes(Paths.get(certFile));
        } catch (IOException ix) {
            log.error("{}", getStackTrace(ix));
            throw new RuntimeException("Failed to read file ", ix);
        }
        this.transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(authProvider)
                .withSecureConnection(cert)
                .build();
        this.tableClient = TableClient.newClient(transport).build();
    }

    public GrpcTransport getTransport() {
        return transport;
    }

    public TableClient getTableClient() {
        return tableClient;
    }

    public void close() {
        try {
            tableClient.close();
            transport.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close transport", e);
        }
    }

    public static void main(String[] args) {
        String f = System.getenv("YDB_ACCESS_CERT_FILE");
        YdbClient ydbClient = new YdbClient(args[0], f);

        ExecutorService service = Executors.newFixedThreadPool(21);
        for (int i = 0; i < 20; i++) {
            service.submit(() -> {
                SessionRetryContext retryCtx = SessionRetryContext.create(ydbClient.getTableClient()).build();
                ydbClient.selectSimple(retryCtx);
            });
        }

        service.shutdown();
        service.close();
        ydbClient.close();
    }

    private void selectSimple(SessionRetryContext retryCtx) {
        String query
                = "SELECT series_id, title, release_date "
                + "FROM series WHERE series_id = " + Thread.currentThread().threadId() % 3;
        TxControl<?> txControl = TxControl.serializableRw().setCommitTx(true);
        DataQueryResult result = retryCtx.supplyResult(session -> session.executeDataQuery(query, txControl))
                .join().getValue();

        ResultSetReader rs = result.getResultSet(0);
        while (rs.next()) {
            log.info("read series with id {}, title {} and release_date {}",
                    rs.getColumn("series_id").getUint64(),
                    rs.getColumn("title").getText(),
                    rs.getColumn("release_date").getDate()
            );
        }
    }

}
