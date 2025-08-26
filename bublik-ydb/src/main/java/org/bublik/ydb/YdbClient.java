package org.bublik.ydb;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.KeyBound;
import tech.ydb.table.description.KeyRange;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.settings.DescribeTableSettings;
import tech.ydb.table.transaction.TableTransaction;
import tech.ydb.table.values.PrimitiveValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

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

    public void describeTables() {
        Arrays.asList("string2", "document2").forEach(tableName -> {
            String tablePath = "/Root/tecm/" + tableName;
            DescribeTableSettings tableSettings = new DescribeTableSettings();
            tableSettings.setIncludeShardKeyBounds(true);
            tableSettings.setIncludeTableStats(true);
            tableSettings.setIncludePartitionStats(true);
            TableDescription tableDesc = retryCtx
                    .supplyResult(session -> session.describeTable(tablePath, tableSettings))
                    .join()
                    .getValue();

            List<String> primaryKeys = tableDesc.getPrimaryKeys();
            List<KeyRange> keyRanges = tableDesc.getKeyRanges();
            List<TableDescription.PartitionStats> partitionStats = tableDesc.getPartitionStats();
            TableDescription.TableStats tableStats = tableDesc.getTableStats();

            log.info("  table {}", tableName);
            for (TableColumn column : tableDesc.getColumns()) {
                boolean isPrimary = primaryKeys.contains(column.getName());
                log.info("     {}: {} {}", column.getName(), column.getType(), isPrimary ? " (PK)" : "");
            }

            log.info("  number of ranges = {}", keyRanges.size());

            keyRanges
                    .forEach(keyRange -> {
                        Optional<KeyBound> from = keyRange.getFrom();
                        Optional<KeyBound> to = keyRange.getTo();
                        log.info("  KeyRange: {} {} - {} {}",
                                from.map(KeyBound::getValue).orElse(null),
                                from.map(KeyBound::isInclusive),
                                to.map(KeyBound::getValue).orElse(null),
                                to.map(KeyBound::isInclusive));
                    });

        });
    }


    public static void main(String[] args) {
        String f = System.getenv("YDB_ACCESS_CERT_FILE");
        YdbClient ydbClient = new YdbClient(args[0], f);

        ydbClient.describeTables();

/*
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
*/

        ydbClient.close();
    }
}
