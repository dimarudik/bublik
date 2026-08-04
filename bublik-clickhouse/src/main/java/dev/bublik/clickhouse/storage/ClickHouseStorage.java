package dev.bublik.clickhouse.storage;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.DataStreamWriter;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import com.google.common.io.LittleEndianDataOutputStream;
import dev.bublik.clickhouse.model.TransferPlan;
import dev.bublik.clickhouse.service.ColumnTransfer;
import dev.bublik.clickhouse.service.ValueTransfer;
import dev.bublik.core.model.*;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import dev.bublik.core.util.Utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

public class ClickHouseStorage extends ClickStorage {
    private ByteArrayOutputStream bufferStream;
    private LittleEndianDataOutputStream leOut;
    private ValueTransfer[] pushPlan;
    private long rowCount = 0;

    public ClickHouseStorage(StorageClass storageClass,
                             ConnectionProperty connectionProperty,
                             Table outboxTable) {
        super(storageClass, connectionProperty, outboxTable);
    }

    private ClickHouseStorage(Builder builder) {
        super(builder);
    }

    public static class Builder extends ClickStorage.Builder<ClickHouseStorage, Builder> {

        public Builder(Client client) {
            super(client);
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public ClickHouseStorage build() {
            validate();
            return new ClickHouseStorage(this);
        }
    }

    @Override
    public <K, T, S extends AutoCloseable, R> LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        Storage sourceStorage = chunk.getSourceStorage();
        if (sourceStorage instanceof JDBCStorage) {
            return jdbcToClickHouse(chunk);
        } else {
            return sourceStorage.transfer(chunk, tableName);
        }
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    public LogMessage jdbcToClickHouse(Chunk<?, ?, ?, ?> chunk) {
        long start = System.currentTimeMillis();
        Client client = getSession();
        Table targetTable = chunk.getT2t().targetTable();
        TableSchema targetTableSchema = client.getTableSchema(
                targetTable.getTableName(), targetTable.getSchemaName());

        List<Column2Column> sortedColumn2Columns = chunk.getT2t().getSortedColumn2ColumnByTargetColumnPosition();

        List<String> targetColumnNames = sortedColumn2Columns.stream()
                .map(c2c -> c2c.targetColumn().columnName())
                .toList();

        TransferPlan plan = buildTransferPlan(targetTableSchema, sortedColumn2Columns);
        ResultSet rs = (ResultSet) chunk.getResultSet();
        DataStreamWriter writer = getWriter(rs, targetTableSchema, plan);

        InsertSettings settings = new InsertSettings();

        try (InsertResponse response = client.insert(
                targetTable.getSchemaName() + "." + targetTable.getTableName(),
                targetColumnNames,
                writer,
                ClickHouseFormat.RowBinary,
                settings
        ).join()) {

            long writtenRows = response.getWrittenRows();
            chunk.setCopied((int) writtenRows);
            long stop = System.currentTimeMillis();

            return new LogMessage(start, stop, "JDBC -> ClickHouse. Rows copied: " + writtenRows);

        } catch (Exception e) {
            Throwable cause = (e.getCause() != null) ? e.getCause() : e;
            throw new RuntimeException("ClickHouse insert failed: " + cause.getMessage(), cause);
        }
    }

    public DataStreamWriter getWriter(
            ResultSet rs,
            TableSchema tableSchema,
            TransferPlan plan) {

        return new DataStreamWriter() {
            @Override
            public void onOutput(OutputStream out) throws IOException {
                ColumnTransfer[] transfers = plan.transfers();
                int columnsCount = transfers.length;

                com.google.common.io.LittleEndianDataOutputStream leOut =
                        new com.google.common.io.LittleEndianDataOutputStream(out);

                try {
                    while (rs.next()) {
                        for (int i = 0; i < columnsCount; i++) {
                            transfers[i].transfer(rs, (i + 1), leOut);
                        }
                    }
                    leOut.flush();
                } catch (Exception e) {
                    log.error("Error during binary streaming: {}", Utils.getStackTrace(e));
                    throw new IOException("Error processing RowBinary transfer", e);
                }
            }

            @Override
            public void onRetry() throws IOException {
                throw new IOException("Retry is not supported for forward-only ResultSet");
            }
        };
    }

    private TransferPlan buildTransferPlan(
            TableSchema tableSchema,
            List<Column2Column> sortedColumn2Columns) {

        int targetColumnsCount = sortedColumn2Columns.size();
        ColumnTransfer[] transfers = new ColumnTransfer[targetColumnsCount];
        String[] sourceColumnNames = new String[targetColumnsCount];

        for (int i = 0; i < targetColumnsCount; i++) {
            Column2Column c2c = sortedColumn2Columns.get(i);
            sourceColumnNames[i] = c2c.sourceColumn().columnName();

            String targetColumnName = c2c.targetColumn().columnName();
            if (targetColumnName.startsWith("\"") &&
                    targetColumnName.endsWith("\"") &&
                    targetColumnName.length() > 1) {
                targetColumnName = targetColumnName.substring(
                        1, targetColumnName.length() - 1);
            }

            ClickHouseColumn chColumn = tableSchema.getColumnByName(targetColumnName);
            final boolean isNullable = chColumn.isNullable();
            String baseTypeName = chColumn.getDataType().getName().toLowerCase();

            final int scale = chColumn.getScale();
            final int precision = chColumn.getPrecision();
            final int fixedLength = "fixedstring".equals(baseTypeName) ? precision : chColumn.getEstimatedLength();

            switch (baseTypeName) {

                case "int8", "uint8": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        // Страховка для NOT NULL
                        if (v == null) {
                            out.writeByte(0);
                            return;
                        }

                        switch (v) {
                            case Number number -> out.writeByte(number.byteValue());
                            case Boolean bool -> out.writeByte(bool ? 1 : 0);
                            default -> {
                                try {
                                    out.writeByte(Byte.parseByte(v.toString().trim()));
                                } catch (Exception ex) {
                                    out.writeByte(0); // ГАРАНТИРУЕМ ЗАПИСЬ 1 БАЙТА ПРИ ЛЮБОМ СБОЕ!
                                }
                            }
                        }
                    };
                    break;
                }

                case "int16", "uint16": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        // Страховка для NOT NULL
                        if (v == null) {
                            out.writeShort((short) 0);
                            return;
                        }

                        switch (v) {
                            case Number number -> out.writeShort(number.shortValue());
                            case Boolean bool -> out.writeShort((short) (bool ? 1 : 0));
                            default -> {
                                try {
                                    out.writeShort(Short.parseShort(v.toString().trim()));
                                } catch (Exception ex) {
                                    out.writeShort((short) 0); // ГАРАНТИРУЕМ ЗАПИСЬ 2 БАЙТ ПРИ ЛЮБОМ СБОЕ!
                                }
                            }
                        }
                    };
                    break;
                }

                case "int32", "uint32": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);

                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1); // Записали маркер NULL
                                return;           // Выходим
                            }
                            out.writeByte(0);     // Маркер присутствия значения
                        }

                        if (v == null) {
                            out.writeInt(0);
                            return;
                        }

                        switch (v) {
                            case BigDecimal bd -> out.writeInt(bd.intValue());
                            case Number number -> out.writeInt(number.intValue());
                            case Boolean bool -> out.writeInt(bool ? 1 : 0);
                            default -> {
                                try {
                                    out.writeInt(Integer.parseInt(v.toString().trim()));
                                } catch (Exception ex) {
                                    out.writeInt(0); // Защита при ошибках парсинга текста
                                }
                            }
                        }
                    };
                    break;
                }

                case "int64", "uint64": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        if (v == null) {
                            out.writeLong(0L);
                            return;
                        }

                        switch (v) {
                            case java.math.BigDecimal bd -> out.writeLong(bd.longValue());
                            case Number number -> out.writeLong(number.longValue());
                            case Boolean bool -> out.writeLong(bool ? 1L : 0L);
                            default -> {
                                try {
                                    out.writeLong(Long.parseLong(v.toString().trim()));
                                } catch (Exception ex) {
                                    out.writeLong(0L);
                                }
                            }
                        }
                    };
                    break;
                }

                case "int128", "uint128": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        java.math.BigInteger bigInt;
                        switch (v) {
                            case null -> bigInt = java.math.BigInteger.ZERO;
                            case java.math.BigInteger bi -> bigInt = bi;
                            case Number num -> bigInt = new java.math.BigInteger(num.toString());
                            default -> {
                                try {
                                    bigInt = new java.math.BigInteger(v.toString().trim());
                                } catch (Exception ex) {
                                    bigInt = java.math.BigInteger.ZERO;
                                }
                            }
                        }

                        byte[] rawBytes = bigInt.toByteArray();
                        // Строго по спецификации RowBinary: 16 байт для Int128/UInt128
                        byte[] finalBytes = new byte[16];
                        byte signByte = (byte) (bigInt.signum() < 0 ? 0xFF : 0x00);
                        java.util.Arrays.fill(finalBytes, signByte);

                        int bytesToCopy = Math.min(rawBytes.length, 16);
                        for (int j = 0; j < bytesToCopy; j++) {
                            finalBytes[j] = rawBytes[rawBytes.length - 1 - j];
                        }
                        out.write(finalBytes);
                    };
                    break;
                }

                case "int256", "uint256": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        java.math.BigInteger bigInt;
                        switch (v) {
                            case null -> bigInt = java.math.BigInteger.ZERO;
                            case java.math.BigInteger bi -> bigInt = bi;
                            case Number num -> bigInt = new java.math.BigInteger(num.toString());
                            default -> {
                                try {
                                    bigInt = new java.math.BigInteger(v.toString().trim());
                                } catch (Exception ex) {
                                    bigInt = java.math.BigInteger.ZERO;
                                }
                            }
                        }

                        byte[] rawBytes = bigInt.toByteArray();
                        // Строго по спецификации RowBinary: 32 байта для Int256/UInt256
                        byte[] finalBytes = new byte[32];
                        byte signByte = (byte) (bigInt.signum() < 0 ? 0xFF : 0x00);
                        java.util.Arrays.fill(finalBytes, signByte);

                        int bytesToCopy = Math.min(rawBytes.length, 32);
                        for (int j = 0; j < bytesToCopy; j++) {
                            finalBytes[j] = rawBytes[rawBytes.length - 1 - j];
                        }
                        out.write(finalBytes);
                    };
                    break;
                }

                case "float32": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        switch (v) {
                            case null -> {
                                out.writeFloat(0.0f);
                                return;
                            }
                            case Boolean bool -> out.writeFloat(bool ? 1.0f : 0.0f);
                            default -> {
                                try {
                                    // НАДЁЖНО: парсим из строки, уничтожая округления ojdbc!
                                    out.writeFloat(Float.parseFloat(v.toString().trim()));
                                } catch (Exception ex) {
                                    out.writeFloat(0.0f);
                                }
                            }
                        }
                    };
                    break;
                }

                case "float64": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        switch (v) {
                            case null -> {
                                out.writeDouble(0.0);
                                return;
                            }
                            case Boolean bool -> out.writeDouble(bool ? 1.0 : 0.0);
                            default -> {
                                try {
                                    out.writeDouble(Double.parseDouble(v.toString().trim()));
                                } catch (Exception ex) {
                                    out.writeDouble(0.0);
                                }
                            }
                        }
                    };
                    break;
                }

                case "bfloat16": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        float fVal;
                        switch (v) {
                            case null -> fVal = 0.0f;
                            case Number number -> fVal = number.floatValue();
                            case Boolean bool -> fVal = bool ? 1.0f : 0.0f;
                            default -> {
                                try {
                                    fVal = Float.parseFloat(v.toString().trim());
                                } catch (Exception ex) {
                                    fVal = 0.0f;
                                }
                            }
                        }

                        int bits = Float.floatToIntBits(fVal);
                        int bfloatBits = bits >>> 16;
                        out.writeShort((short) bfloatBits);
                    };
                    break;
                }

                case "decimal": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        BigDecimal bd;
                        switch (v) {
                            case null -> bd = BigDecimal.ZERO;
                            case BigDecimal bigDecimal -> bd = bigDecimal;
                            case Number num -> bd = new BigDecimal(num.toString());
                            default -> {
                                try {
                                    bd = new BigDecimal(v.toString().trim());
                                } catch (Exception ex) {
                                    bd = BigDecimal.ZERO;
                                }
                            }
                        }

                        BigDecimal moved = bd.movePointRight(scale);
                        java.math.BigInteger bigInt = moved.setScale(0,
                                java.math.RoundingMode.HALF_UP).toBigInteger();

                        byte[] rawBytes = bigInt.toByteArray();

                        int byteWidth = 8;
                        if (precision <= 9) byteWidth = 4;
                        else if (precision <= 18) byteWidth = 8;
                        else if (precision <= 38) byteWidth = 16;
                        else byteWidth = 32;

                        byte[] finalBytes = new byte[byteWidth];

                        byte signByte = (byte) (bigInt.signum() < 0 ? 0xFF : 0x00);
                        java.util.Arrays.fill(finalBytes, signByte);

                        int bytesToCopy = Math.min(rawBytes.length, byteWidth);
                        for (int j = 0; j < bytesToCopy; j++) {
                            finalBytes[j] = rawBytes[rawBytes.length - 1 - j];
                        }

                        out.write(finalBytes);
                    };
                    break;
                }

                case "string": {
                    final String finalSrcName = c2c.sourceColumn().columnName();
                    final String srcType = c2c.sourceColumn().columnType().toLowerCase();


                    transfers[i] = (r, jdbcIdx, out) -> {

                        Object v = null;
                        try {
                            v = r.getObject(jdbcIdx);
                        } catch (Exception ex) {
                            v = null;
                        }

                        // ПУЛЕНЕПРОБИВАЕМАЯ ОЧИСТКА НА САМОМ ВХОДЕ:
                        // Сначала вырезаем бинарные нули и пробелы Oracle, которые маскируют NULL!
                        if (v instanceof String s) {
                            String cleaned = s.replace("\u0000", "").trim();
                            if (cleaned.isEmpty()) {
                                v = null; // Если осталась пустота — это честный NULL
                            } else {
                                v = cleaned; // Иначе сохраняем очищенную строку
                            }
                        }

                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1); // NULL-маркер ClickHouse (строго 1 байт)
                                return;           // Немедленный выход!
                            }
                            out.writeByte(0);     // Маркер присутствия значения (0x00)
                        }

                        if (v == null) {
                            out.writeByte(0);     // Страховка для NOT NULL
                            return;
                        }

                        byte[] bytes;
                        try {
                            if (srcType.contains("raw") || srcType.contains("bytea") || v instanceof byte[]) {
                                byte[] rawBytes = (v instanceof byte[]) ? (byte[]) v : r.getBytes(finalSrcName);
                                if (rawBytes != null && rawBytes.length > 0) {
                                    String hexStr = java.util.HexFormat.of().withUpperCase().formatHex(rawBytes);
                                    bytes = hexStr.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                                } else {
                                    bytes = new byte[0];
                                }
                            } else if (srcType.contains("blob")) {
                                java.sql.Blob blob = r.getBlob(finalSrcName);
                                if (blob != null && blob.length() > 0) {
                                    byte[] blobBytes = blob.getBytes(1, (int) blob.length());
                                    String hexStr = java.util.HexFormat.of().withUpperCase().formatHex(blobBytes);
                                    bytes = hexStr.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                                } else {
                                    bytes = new byte[0];
                                }
                            } else if (v instanceof java.sql.Clob clob) {
                                long clobLen = clob.length();
                                if (clobLen > 0) {
                                    String s = clob.getSubString(1, (int) clobLen);
                                    s = (s != null) ? s.replace("\u0000", "").trim() : "";
                                    bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                                } else {
                                    bytes = new byte[0];
                                }
                            } else {
                                // Поскольку строка ОЧИЩЕНА на самом верху, здесь просто берем байты текста!
                                bytes = v.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                            }
                        } catch (Exception ex) {
                            bytes = new byte[0];
                        }

                        // Расчет и запись Varint-префикса длины
                        long val = bytes.length;
                        while ((val & 0xFFFFFFFFFFFFFF80L) != 0L) {
                            out.writeByte(((int) val & 0x7F) | 0x80);
                            val >>>= 7;
                        }
                        out.writeByte((int) val & 0x7F);

                        // Запись тела строки
                        if (bytes.length > 0) {
                            out.write(bytes);
                        }
                    };
                    break;
                }

                case "fixedstring": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = null;
                        try {
                            v = r.getObject(jdbcIdx);
                        } catch (Exception ex) {
                            v = null;
                        }

                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1); // NULL маркер Кликхауса
                                return;
                            }
                            out.writeByte(0); // Значение присутствует
                        }

                        // Локальный буфер строго заданной ширины
                        byte[] finalBytes = new byte[fixedLength];

                        if (v != null) {
                            try {
                                byte[] srcBytes;
                                if (v instanceof byte[] byteArray) {
                                    srcBytes = byteArray;
                                } else {
                                    srcBytes = v.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                                }

                                // Безопасное копирование данных в буфер
                                System.arraycopy(srcBytes, 0, finalBytes, 0,
                                        Math.min(srcBytes.length, fixedLength));
                            } catch (Exception ex) {
                                // При любом сбое массив останется заполнен нулевыми байтами 0x00
                            }
                        }

                        // КРИТИЧЕСКИЙ ФИКС: Пишем массив СТРОГО ПОБАЙТОВО через writeByte!
                        // Это гарантирует, что в сокет улетит ровно fixedLength байт (для CHAR(4) это 4 байта)
                        // без сбоев буферизации массивов в Google Guava!
                        for (int bIdx = 0; bIdx < fixedLength; bIdx++) {
                            out.writeByte(finalBytes[bIdx]);
                        }
                    };
                    break;
                }

                case "uuid": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        String uuidStr = v.toString().trim().replace("-", "");
                        if (uuidStr.length() != 32) {
                            out.writeLong(0L);
                            out.writeLong(0L);
                            return;
                        }

                        long mostSig = Long.parseUnsignedLong(uuidStr.substring(0, 16), 16);
                        long leastSig = Long.parseUnsignedLong(uuidStr.substring(16, 32), 16);

                        out.writeLong(mostSig);
                        out.writeLong(leastSig);
                    };
                    break;
                }

                case "datetime": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        java.sql.Timestamp ts;
                        if (v instanceof java.sql.Timestamp timestamp) {
                            ts = timestamp;
                        } else if (v instanceof java.sql.Date date) {
                            ts = new java.sql.Timestamp(date.getTime());
                        } else if (v instanceof java.time.LocalDateTime ldt) {
                            ts = java.sql.Timestamp.valueOf(ldt);
                        } else {
                            try {
                                ts = java.sql.Timestamp.valueOf(v.toString().trim());
                            } catch (Exception ex) {
                                ts = new java.sql.Timestamp(0L);
                            }
                        }

                        int seconds = (int) (ts.getTime() / 1000);
                        out.writeInt(seconds);
                    };
                    break;
                }

                case "datetime64": {
                    final int targetScale = scale;

                    transfers[i] = (r, jdbcIdx, out) -> {
                        java.sql.Timestamp ts = null;
                        try {
                            java.util.Calendar utcCal = java.util.Calendar.getInstance(
                                    java.util.TimeZone.getTimeZone("UTC"));
                            ts = r.getTimestamp(jdbcIdx, utcCal);
                        } catch (Exception ex) {
                            Object v = r.getObject(jdbcIdx);
                            if (v instanceof java.sql.Timestamp timestamp) {
                                ts = timestamp;
                            } else if (v instanceof java.time.OffsetDateTime odt) {
                                ts = java.sql.Timestamp.from(odt.toInstant());
                            }
                        }

                        if (isNullable) {
                            if (ts == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        if (ts == null) {
                            out.writeLong(0L);
                            return;
                        }

                        long seconds = ts.getTime() / 1000;
                        int nanos = ts.getNanos();

                        long totalUnits;
                        if (targetScale == 0) {
                            totalUnits = seconds; // ВОЗВРАЩАЕМ ЧИСТЫЕ СЕКУНДЫ! Даст 3000-01-01
                        } else if (targetScale == 6) {
                            totalUnits = (seconds * 1_000_000L) + (nanos / 1000);
                        } else {
                            totalUnits = (seconds * (long) Math.pow(10, targetScale))
                                    + (nanos / (long) Math.pow(10, 9 - targetScale));
                        }

                        out.writeLong(totalUnits);
                    };
                    break;
                }

/*
                case "datetime64": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        java.sql.Timestamp ts = null;
                        try {
                            java.util.Calendar utcCal = java.util.Calendar.getInstance(
                                    java.util.TimeZone.getTimeZone("UTC"));
                            ts = r.getTimestamp(jdbcIdx, utcCal);
                        } catch (Exception ex) {
                            Object v = r.getObject(jdbcIdx);
                            if (v instanceof java.sql.Timestamp timestamp) {
                                ts = timestamp;
                            } else if (v instanceof java.time.OffsetDateTime odt) {
                                ts = java.sql.Timestamp.from(odt.toInstant());
                            }
                        }

                        if (isNullable) {
                            if (ts == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        if (ts == null) {
                            ts = new java.sql.Timestamp(0L);
                        }

                        long millis = ts.getTime();
                        long microseconds = millis * 1000;

                        int nanos = ts.getNanos();
                        long extraMicros = (nanos % 1000000) / 1000;
                        long totalMicros = microseconds + extraMicros;

                        out.writeLong(totalMicros);
                    };
                    break;
                }
*/

                case "date": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);

                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        if (v == null) {
                            out.writeShort((short) 0);
                            return;
                        }

                        LocalDate localDate;
                        try {
                            if (v instanceof java.sql.Date sd) {
                                localDate = sd.toLocalDate();
                            } else if (v instanceof java.sql.Timestamp ts) {
                                localDate = ts.toLocalDateTime().toLocalDate();
                            } else if (v instanceof java.time.LocalDate ld) {
                                localDate = ld;
                            } else {
                                String str = v.toString().trim();
                                if (str.length() >= 10) {
                                    localDate = java.time.LocalDate.parse(str.substring(0, 10));
                                } else {
                                    localDate = java.time.LocalDate.of(1970, 1, 1);
                                }
                            }
                        } catch (Exception ex) {
                            localDate = java.time.LocalDate.of(1970, 1, 1);
                        }

                        long daysLong = java.time.temporal.ChronoUnit.DAYS.between(
                                java.time.LocalDate.of(1970, 1, 1), localDate);

                        if (daysLong < -65535 || daysLong > 65535) {
                            daysLong = 0;
                        }

                        out.writeShort((short) daysLong);
                    };
                    break;
                }

                case "date32": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);

                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        if (v == null) {
                            out.writeInt(0);
                            return;
                        }

                        LocalDate localDate;
                        try {
                            if (v instanceof java.sql.Date sd) {
                                localDate = sd.toLocalDate();
                            } else if (v instanceof java.sql.Timestamp ts) {
                                localDate = ts.toLocalDateTime().toLocalDate();
                            } else if (v instanceof java.time.LocalDate ld) {
                                localDate = ld;
                            } else {
                                String str = v.toString().trim();
                                if (str.length() >= 10) {
                                    localDate = java.time.LocalDate.parse(str.substring(0, 10));
                                } else {
                                    localDate = java.time.LocalDate.of(1970, 1, 1);
                                }
                            }
                        } catch (Exception ex) {
                            localDate = java.time.LocalDate.of(1970, 1, 1);
                        }

                        long daysLong = java.time.temporal.ChronoUnit.DAYS.between(
                                java.time.LocalDate.of(1970, 1, 1), localDate);

                        // УБРАЛИ СРЕЗЫ: Свободно пишем честные Int32 дни для 3000 года!
                        out.writeInt((int) daysLong);
                    };
                    break;
                }

                default:
                    transfers[i] = (r, jdbcIdx, out) -> {};
                    break;
            }
        }
        return new TransferPlan(transfers, sourceColumnNames, null);
    }

    @Override
    public <K, T, S extends AutoCloseable, R, V>  void insertColumnValue(List<ColumnValue<V>> columnValues,
                                                                         Chunk<K, T, S, R> chunk) {
        try {
            if (bufferStream == null) {
                bufferStream = new ByteArrayOutputStream(1024 * 1024); // 1 MB начальный буфер
                leOut = new LittleEndianDataOutputStream(bufferStream);

                Client client = getSession();
                Table targetTable = chunk.getT2t().targetTable();
                TableSchema targetTableSchema = client.getTableSchema(
                        targetTable.getTableName(), targetTable.getSchemaName());

                pushPlan = buildPushTransferPlan(targetTableSchema, columnValues);
            }

            int columnsCount = columnValues.size();
            for (int i = 0; i < columnsCount; i++) {
                Object value = columnValues.get(i).value();
                pushPlan[i].transfer(value, leOut);
            }

            rowCount++;

            if (rowCount >= 50000) {
                flushBuffer(chunk);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to process push row in ClickHouseStorage", e);
        }
    }

    @Override
    public <K, T, S extends AutoCloseable, R> void flushBuffer(Chunk<K, T, S, R> chunk) {
        if (rowCount == 0 || bufferStream == null) return;

//        log.info("flushBuffer called. Current rowCount: {}, bufferStream is null: {}",
//                this.rowCount, (this.bufferStream == null));

        long start = System.currentTimeMillis();
        Client client = getSession();
        Table targetTable = chunk.getT2t().targetTable();

        List<Column2Column> sortedColumn2Columns = chunk.getT2t().getSortedColumn2ColumnByTargetColumnPosition();
        List<String> targetColumnNames = sortedColumn2Columns.stream()
                .map(c2c -> c2c.targetColumn().columnName())
                .toList();

        try {
            leOut.flush();
            byte[] binaryData = bufferStream.toByteArray();

            InsertSettings settings = new InsertSettings();
            settings.serverSetting("insert_deduplicate", "1");

            try (InsertResponse response = client.insert(
                    targetTable.getSchemaName() + "." + targetTable.getTableName(),
                    targetColumnNames,
                    new DataStreamWriter() {
                        @Override
                        public void onOutput(OutputStream out) throws IOException {
                            out.write(binaryData);
                        }
                        @Override
                        public void onRetry() throws IOException {
                            throw new IOException("Retry not supported for raw memory buffer");
                        }
                    },
                    ClickHouseFormat.RowBinary,
                    settings
            ).join()) {

                long stop = System.currentTimeMillis();
                log.info("Push CDC -> ClickHouse success. Rows: {}, Size: {} bytes, Time: {} ms",
                        rowCount, binaryData.length, (stop - start));
            }

        } catch (Exception e) {
            throw new RuntimeException("ClickHouse bundle push failed: " + e.getMessage(), e);
        } finally {
            bufferStream = null;
            leOut = null;
            pushPlan = null;
            rowCount = 0;
        }
    }

    private <V> ValueTransfer[] buildPushTransferPlan(
            TableSchema tableSchema,
            List<ColumnValue<V>> columnValues) {

        int targetColumnsCount = columnValues.size();
        ValueTransfer[] transfers = new ValueTransfer[targetColumnsCount];

        for (int i = 0; i < targetColumnsCount; i++) {
            ColumnValue<?> cv = columnValues.get(i);
            String targetColumnName = cv.targetColumn().columnName();

            if (targetColumnName.startsWith("\"") &&
                    targetColumnName.endsWith("\"") &&
                    targetColumnName.length() > 1) {
                targetColumnName = targetColumnName.substring(
                        1, targetColumnName.length() - 1);
            }

            ClickHouseColumn chColumn = tableSchema.getColumnByName(targetColumnName);
            final boolean isNullable = chColumn.isNullable();
            String baseTypeName = chColumn.getDataType().getName().toLowerCase();

            switch (baseTypeName) {

                case "int32", "uint32": {
                    transfers[i] = (v, out) -> {
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }
                        switch (v) {
                            case null -> {
                                out.writeInt(0);
                                return;
                            }
                            case Number number -> out.writeInt(number.intValue());
                            case Boolean bool -> out.writeInt(bool ? 1 : 0);
                            default -> {
                                try {
                                    out.writeInt(Integer.parseInt(v.toString().trim()));
                                } catch (Exception ex) {
                                    out.writeInt(0);
                                }
                            }
                        }
                    };
                    break;
                }

                case "string": {
                    transfers[i] = (v, out) -> {
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1);
                                return;
                            }
                            out.writeByte(0);
                        }

                        byte[] bytes;
                        switch (v) {
                            case null -> bytes = new byte[0];
                            case byte[] byteArray -> bytes = byteArray;
                            default -> {
                                String s = v.toString().replace("\u0000", "");
                                bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                            }
                        }

                        long val = bytes.length;
                        while ((val & 0xFFFFFFFFFFFFFF80L) != 0L) {
                            out.writeByte(((int) val & 0x7F) | 0x80);
                            val >>>= 7;
                        }
                        out.writeByte((int) val & 0x7F);

                        out.write(bytes);
                    };
                    break;
                }

                default:
                    transfers[i] = (v, out) -> {};
                    break;
            }
        }
        return transfers;
    }
}
