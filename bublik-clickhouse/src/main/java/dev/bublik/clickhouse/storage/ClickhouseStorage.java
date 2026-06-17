package dev.bublik.clickhouse.storage;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.DataStreamWriter;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;
import dev.bublik.clickhouse.model.TransferPlan;
import dev.bublik.clickhouse.service.ColumnTransfer;
import dev.bublik.core.model.*;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static dev.bublik.core.util.Utils.getStackTrace;

public class ClickhouseStorage<K, T, S extends Client, R> extends ClickStorage<K, T, S, R> {
    public ClickhouseStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
    }

    @Override
    public LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        Storage<K, T, S, R> sourceStorage = chunk.getSourceStorage();
        if (sourceStorage instanceof ClickStorage<K,T,S,R>) {
            if (chunk.getTargetStorage() instanceof JDBCStorage<?, ?, ?, ?>) {
                return new LogMessage(0, 0, "ClickHouse -> JDBC");
            } else {
                return new LogMessage(0, 0, "ClickHouse -> ClickHouse");
            }
        } else if (sourceStorage instanceof JDBCStorage) {
            ResultSet resultSet = (ResultSet) chunk.getResultSet();
            return jdbcToClickHouse(chunk, resultSet);
        }
        throw new RuntimeException("Unknown storage type");
    }

    public LogMessage jdbcToClickHouse(Chunk<K, T, S, R> chunk, ResultSet rs) {
        long start = System.currentTimeMillis();
        Client client = getSession();
        Table<?> targetTable = chunk.getT2t().targetTable();
        TableSchema targetTableSchema = client.getTableSchema(targetTable.getTableName(), targetTable.getSchemaName());

        List<Column2Column> column2Columns = chunk.getT2t().column2Columns();
        List<String> targetColumnNames = column2Columns.stream()
                .map(c2c -> c2c.targetColumn().columnName())
                .toList();

        // 1. Генерируем план вставки через наш новый изолированный метод со switch-case
        TransferPlan plan = buildTransferPlan(targetTableSchema, column2Columns);

        // 2. Передаем этот план в getWriter
        DataStreamWriter writer = getWriter(rs, targetTableSchema, plan);

        CompletableFuture<InsertResponse> future = client.insert(
                targetTable.getSchemaName() + "." + targetTable.getTableName(),
                targetColumnNames,
                writer,
                ClickHouseFormat.RowBinary,
                new InsertSettings()
        );

        try (InsertResponse response = future.get()) {
            long writtenRows = response.getWrittenRows();
            chunk.setCopied((int) writtenRows);
            long stop = System.currentTimeMillis();
            return new LogMessage(start, stop, "JDBC -> ClickHouse. Rows copied: " + writtenRows);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Transfer interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new RuntimeException("ClickHouse insert failed: " + cause.getMessage(), cause);
        }
    }

    public DataStreamWriter getWriter(ResultSet rs, TableSchema tableSchema, TransferPlan plan) {
        return new DataStreamWriter() {
            @Override
            public void onOutput(OutputStream out) throws IOException {
                RowBinaryFormatWriter writer = new RowBinaryFormatWriter(out, tableSchema, ClickHouseFormat.RowBinary);

                ColumnTransfer[] transfers = plan.transfers();
                String[] sourceColumnNames = plan.sourceColumnNames();
                int[] chIndices = plan.chIndices();
                int columnsCount = transfers.length;

                try {
                    while (rs.next()) {
                        for (int i = 0; i < columnsCount; i++) {
                            transfers[i].transfer(rs, sourceColumnNames[i], writer, chIndices[i]);
                        }
                        writer.commitRow();
                    }
                } catch (Exception e) {
                    log.error("Error during streaming: {}", getStackTrace(e));
                    throw new IOException("Error processing high-speed RowBinary transfer", e);
                }
            }

            @Override
            public void onRetry() throws IOException {
                throw new IOException("Retry is not supported for forward-only ResultSet");
            }
        };
    }

    private TransferPlan buildTransferPlan(TableSchema tableSchema, List<Column2Column> column2Columns) {
        int targetColumnsCount = column2Columns.size();

        ColumnTransfer[] transfers = new ColumnTransfer[targetColumnsCount];
        int[] chIndices = new int[targetColumnsCount];
        String[] sourceColumnNames = new String[targetColumnsCount];

        for (int i = 0; i < targetColumnsCount; i++) {
            Column2Column c2c = column2Columns.get(i);

            // Кэшируем имена исходных колонок и целевые бинарные индексы ClickHouse
            sourceColumnNames[i] = c2c.sourceColumn().columnName();
            chIndices[i] = tableSchema.nameToColumnIndex(c2c.targetColumn().columnName());

            // Очищаем имя типа (например, "DateTime64(3)" превращаем в "datetime64", а "Decimal(18,4)" в "decimal")
            String rawTypeName = c2c.targetColumn().columnType().toLowerCase();
            String baseTypeName = rawTypeName.split("\\(")[0].trim();

            // Современный switch-case, куда теперь очень легко добавлять новые типы
            switch (baseTypeName) {
                case "datetime", "datetime64" -> transfers[i] = (r, srcName, w, chIdx) -> {
                    Timestamp ts = r.getTimestamp(srcName);
                    w.setDateTime(chIdx, ts != null ? ts.toLocalDateTime() : null);
                };

                case "uint64", "int64" -> transfers[i] = (r, srcName, w, chIdx) -> {
                    BigDecimal bd = r.getBigDecimal(srcName);
                    if (bd != null) w.setLong(chIdx, bd.longValue());
                    else w.setValue(chIdx, null);
                };

                case "uint32", "int32" -> transfers[i] = (r, srcName, w, chIdx) -> {
                    BigDecimal bd = r.getBigDecimal(srcName);
                    if (bd != null) w.setInteger(chIdx, bd.intValue());
                    else w.setValue(chIdx, null);
                };

                case "date", "date32" -> transfers[i] = (r, srcName, w, chIdx) -> {
                    java.sql.Date d = r.getDate(srcName);
                    w.setDate(chIdx, d != null ? d.toLocalDate() : null);
                };

                // Пример добавления нового типа в будущем:
                // case "string", "lowcardinality" -> transfers[i] = (r, srcName, w, chIdx) -> w.setString(chIdx, r.getString(srcName));

                default -> transfers[i] = (r, srcName, w, chIdx) -> {
                    w.setValue(chIdx, r.getObject(srcName));
                };
            }
        }

        return new TransferPlan(transfers, sourceColumnNames, chIndices);
    }
}
