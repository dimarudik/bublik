package org.bublik.ydb;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.TableTransaction;
import tech.ydb.table.values.PrimitiveValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.bublik.exception.Utils.getStackTrace;

public class YdbClient {
    private static final Logger log = LoggerFactory.getLogger(YdbClient.class);

    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;

    public YdbClient(String connectionString, String certFile) {
        StaticCredentials authProvider = new StaticCredentials("root", "passw0rd");

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
        this.tableClient = TableClient.newClient(transport).sessionPoolSize(100, 200).build();
        this.retryCtx = SessionRetryContext.create(tableClient).build();
    }

    public void close() {
        try {
            tableClient.close();
            transport.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close transport", e);
        }
    }

    private void tclTransaction() {
        retryCtx.supplyStatus(session -> {
            TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);
            String query
                    = "DECLARE $airDate AS Date; "
//                    + "UPDATE episodes SET air_date = $airDate WHERE title = \"TBD\";";
                    + "UPDATE episodes SET air_date = $airDate WHERE episode_id = " + Thread.currentThread().threadId();
            Params params = Params.of("$airDate", PrimitiveValue.newDate(Instant.now()));
            DataQueryResult result = transaction.executeDataQuery(query, params)
                    .join().getValue();

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            log.info("trx id: {}", result.getTxId());

            return transaction.commit();
        }).join().expectSuccess("tcl transaction problem");
    }

    public static void main(String[] args) {
        String f = System.getenv("YDB_ACCESS_CERT_FILE");
        YdbClient ydbClient = new YdbClient(args[0], f);

        ExecutorService service = Executors.newFixedThreadPool(21);
        for (int i = 0; i < 20; i++) {
            int finalI = i;
            service.submit(() -> {
                log.info("Submitting task {}", finalI + 1);
                ydbClient.tclTransaction();
                log.info("");
            });
        }

        service.shutdown();
        service.close();
        ydbClient.close();
    }
}
