package dev.bublik.clickhouse.storage;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.DataStreamWriter;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import dev.bublik.clickhouse.model.TransferPlan;
import dev.bublik.clickhouse.service.ColumnTransfer;
import dev.bublik.core.model.*;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import dev.bublik.core.util.Utils;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class ClickHouseStorage<K, T, S extends Client, R> extends ClickStorage<K, T, S, R> {
    public ClickHouseStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
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
        TableSchema targetTableSchema = client.getTableSchema(
                targetTable.getTableName(), targetTable.getSchemaName());

//        List<Column2Column> originalColumns = chunk.getT2t().column2Columns();
        List<Column2Column> sortedColumn2Columns = chunk.getT2t().getSortedColumn2ColumnByTargetColumnPosition();

/*
        List<Column2Column> sortedColumn2Columns = originalColumns.stream()
                .sorted(Comparator.comparingInt(c2c -> c2c.targetColumn().columnPosition()))
                .toList();
*/

        List<String> targetColumnNames = sortedColumn2Columns.stream()
                .map(c2c -> c2c.targetColumn().columnName())
                .toList();

        TransferPlan plan = buildTransferPlan(targetTableSchema, sortedColumn2Columns);
        DataStreamWriter writer = getWriter(rs, targetTableSchema, plan);

        InsertSettings settings = new InsertSettings();
/*
        settings.compressClientRequest(false);
        settings.useHttpCompression(false);
        settings.serverSetting("async_insert", "0");
        settings.serverSetting("wait_for_async_insert", "0");
        settings.serverSetting("insert_deduplicate", "0");
*/

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
                        // Отсекаем фантомные зацикленные строки скроллируемого курсора ojdbc
                        for (int i = 0; i < columnsCount; i++) {
                            // Стримим примитивы напрямую в ClickHouse со скоростью процессора
                            transfers[i].transfer(rs, (i + 1), leOut);
                        }
                    }
                    leOut.flush();
                    // Метод close() убран, закрытием сокета управляет HttpClient
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
            final int fixedLength = chColumn.getEstimatedLength();

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
                        switch (v) {
                            case null -> {
                                out.writeByte(0);
                                return;
                            }
                            case Number number -> out.writeByte(number.byteValue());
                            case Boolean bool -> out.writeByte(bool ? 1 : 0);
                            default -> {
                                try {
                                    out.writeByte(Byte.parseByte(v.toString().trim()));
                                } catch (Exception ex) {
                                    out.writeByte(0);
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
                        switch (v) {
                            case null -> {
                                out.writeShort((short) 0);
                                return;
                            }
                            case Number number -> out.writeShort(number.shortValue());
                            case Boolean bool -> out.writeShort((short) (bool ? 1 : 0));
                            default -> {
                                try {
                                    out.writeShort(Short.parseShort(v.toString().trim()));
                                } catch (Exception ex) {
                                    out.writeShort((short) 0);
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
                        switch (v) {
                            case null -> {
                                out.writeLong(0L);
                                return;
                            }
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
                                    // НАДЁЖНО: парсим из строки, уничтожая округления ojdbc!
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
                                out.writeByte(1); // NULL - ничего не следует
                                return;
                            }
                            out.writeByte(0); // Значение присутствует
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

                        // КОНВЕРТАЦИЯ FLOAT32 -> BFLOAT16 STRICTLY BY SPECIFICATION:
                        // 1. Получаем полные 32-битные аппаратно-зависимые биты IEEE 754
                        int bits = Float.floatToIntBits(fVal);

                        // 2. Сдвигаем на 16 бит вправо, оставляя старшие 2 байта (знак + экспонента + мантисса)
                        int bfloatBits = bits >>> 16;

                        // 3. Записываем ровно 2 байта в Little-Endian поток
                        out.writeShort((short) bfloatBits);
                    };
                    break;
                }

                case "decimal": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1); // NULL - ничего не следует
                                return;
                            }
                            out.writeByte(0); // Значение присутствует
                        }

                        BigDecimal bd;
                        // ВАШЕ ОТЛИЧНОЕ УЛУЧШЕНИЕ: Строгое приведение типов через строковое представление
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

                        // Сдвигаем запятую вправо на scale знаков ClickHouse (для 123456.78 при scale=2 станет 12345678)
                        BigDecimal moved = bd.movePointRight(scale);
                        java.math.BigInteger bigInt = moved.setScale(0,
                                java.math.RoundingMode.HALF_UP).toBigInteger();

                        byte[] rawBytes = bigInt.toByteArray();

                        // Вычисляем строгую бинарную ширину Decimal по спецификации ClickHouse
                        int byteWidth = 8; // По умолчанию Decimal64 (8 байт)
                        if (precision <= 9) byteWidth = 4;        // Decimal32 (4 байта)
                        else if (precision <= 18) byteWidth = 8;   // Decimal64 (8 байт)
                        else if (precision <= 38) byteWidth = 16;  // Decimal128 (16 байт)
                        else byteWidth = 32;                       // Decimal256 (32 байта)

                        byte[] finalBytes = new byte[byteWidth];

                        // Заполняем массив с учетом знака (дополнительного кода для отрицательных чисел)
                        byte signByte = (byte) (bigInt.signum() < 0 ? 0xFF : 0x00);
                        java.util.Arrays.fill(finalBytes, signByte);

                        // Переворачиваем байты в формат Little-Endian строго в границах byteWidth!
                        int bytesToCopy = Math.min(rawBytes.length, byteWidth);
                        for (int j = 0; j < bytesToCopy; j++) {
                            finalBytes[j] = rawBytes[rawBytes.length - 1 - j];
                        }

                        // Записываем фиксированное количество байт (4, 8, 16 или 32)
                        out.write(finalBytes);
                    };
                    break;
                }

                case "string": {
                    // Захватываем метаданные сорса на этапе компиляции плана
                    final String finalSrcName = c2c.sourceColumn().columnName();
                    final String srcType = c2c.sourceColumn().columnType().toLowerCase();

                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1); // NULL - дополнительные байты не пишутся
                                return;
                            }
                            out.writeByte(0); // Значение присутствует
                        }

                        byte[] bytes;

                        // 1. ПЕРЕХВАТ ТИПА RAW -> HEX STRING
                        if (srcType.contains("raw")) {
                            byte[] rawBytes = r.getBytes(finalSrcName);
                            if (rawBytes != null && rawBytes.length > 0) {
                                String hexStr = java.util.HexFormat.of()
                                        .withUpperCase()
                                        .formatHex(rawBytes);
                                bytes = hexStr.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                            } else {
                                bytes = new byte[0];
                            }
                        }
                        // 2. ИСПРАВЛЕНИЕ: ПЕРЕХВАТ ТИПА BLOB -> HEX STRING
                        else if (srcType.contains("blob")) {
                            java.sql.Blob blob = r.getBlob(finalSrcName);
                            if (blob != null) {
                                long length = blob.length();
                                if (length > 0) {
                                    byte[] blobBytes = blob.getBytes(1, (int) length);
                                    String hexStr = java.util.HexFormat.of()
                                            .withUpperCase()
                                            .formatHex(blobBytes);
                                    bytes = hexStr.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                                } else {
                                    bytes = new byte[0];
                                }
                            } else {
                                bytes = new byte[0];
                            }
                        }
                        // 3. ПЕРЕХВАТ ТИПА CLOB -> TEXT STRING
                        else if (v instanceof java.sql.Clob clob) {
                            long length = clob.length();
                            if (length > 0) {
                                String s = clob.getSubString(1, (int) length);
                                if (s != null) {
                                    s = s.replace("\u0000", "");
                                    bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                                } else {
                                    bytes = new byte[0];
                                }
                            } else {
                                bytes = new byte[0];
                            }
                        }
                        // 4. СТАНДАРТНЫЕ СТРОКИ VARCHAR2 / NVARCHAR2 / TEXT
                        else {
                            String s = v.toString().replace("\u0000", "");
                            bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        }

                        // Кодируем Varint/LEB128 префикс длины полученной HEX или текстовой строки
                        long val = bytes.length;
                        while ((val & 0xFFFFFFFFFFFFFF80L) != 0L) {
                            out.writeByte(((int) val & 0x7F) | 0x80);
                            val >>>= 7;
                        }
                        out.writeByte((int) val & 0x7F);

                        // Записываем байты текста в сокет ClickHouse
                        out.write(bytes);
                    };
                    break;
                }

                case "fixedstring": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1); // NULL - ничего не следует
                                return;
                            }
                            out.writeByte(0); // Значение присутствует
                        }

                        byte[] srcBytes;
                        switch (v) {
                            case null -> srcBytes = new byte[0];
                            case byte[] byteArray -> srcBytes = byteArray;
                            default -> {
                                String s = v.toString();
                                srcBytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                            }
                        }

                        byte[] finalBytes = new byte[fixedLength];
                        System.arraycopy(srcBytes, 0, finalBytes, 0,
                                Math.min(srcBytes.length, fixedLength));

                        out.write(finalBytes);
                    };
                    break;
                }

                case "uuid": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        Object v = r.getObject(jdbcIdx);
                        if (isNullable) {
                            if (v == null) {
                                out.writeByte(1); // NULL - ничего не следует
                                return;
                            }
                            out.writeByte(0); // Значение присутствует
                        }

                        // Чистим дефисы из строки UUID
                        String uuidStr = v.toString().trim().replace("-", "");
                        if (uuidStr.length() != 32) {
                            out.writeLong(0L);
                            out.writeLong(0L);
                            return;
                        }

                        // ЖЕСТКО ПО ДОКУМЕНТАЦИИ:
                        // Берем первую половину строки (символы 0-16)
                        long mostSig = Long.parseUnsignedLong(uuidStr.substring(0, 16), 16);
                        // Берем вторую половину строки (символы 16-32)
                        long leastSig = Long.parseUnsignedLong(uuidStr.substring(16, 32), 16);

                        // Стримим в leOut: сначала Most, затем Least!
                        // Обертка LittleEndianDataOutputStream сама развернет байты каждой половины!
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
                                out.writeByte(1); // NULL - ничего не следует
                                return;
                            }
                            out.writeByte(0); // Значение присутствует
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
                                ts = new java.sql.Timestamp(0L); // Дефолт: 1970-01-01
                            }
                        }

                        // Переводим миллисекунды Java в секунды Unix для DateTime (4 байта)
                        // Обертка LittleEndianDataOutputStream автоматически запишет их в LE-формате!
                        int seconds = (int) (ts.getTime() / 1000);
                        out.writeInt(seconds);
                    };
                    break;
                }

                case "datetime64": {
                    transfers[i] = (r, jdbcIdx, out) -> {
                        java.sql.Timestamp ts = null;
                        try {
                            // ЭТАЛОННОЕ РЕШЕНИЕ: Вычитываем TIMESTAMP WITH TIME ZONE
                            // строго в контексте UTC-календаря, полностью ликвидируя сдвиги зон!
                            java.util.Calendar utcCal = java.util.Calendar.getInstance(
                                    java.util.TimeZone.getTimeZone("UTC"));
                            ts = r.getTimestamp(jdbcIdx, utcCal);
                        } catch (Exception ex) {
                            // Резервный вариант, если getTimestamp по календарю не поддерживается
                            Object v = r.getObject(jdbcIdx);
                            if (v instanceof java.sql.Timestamp timestamp) {
                                ts = timestamp;
                            } else if (v instanceof java.time.OffsetDateTime odt) {
                                ts = java.sql.Timestamp.from(odt.toInstant());
                            }
                        }

                        if (isNullable) {
                            if (ts == null) {
                                out.writeByte(1); // NULL - ничего не следует
                                return;
                            }
                            out.writeByte(0); // Значение присутствует
                        }

                        if (ts == null) {
                            ts = new java.sql.Timestamp(0L);
                        }

                        // Переводим чистые UTC-миллисекунды в микросекунды для DateTime64(6)
                        long millis = ts.getTime();
                        long microseconds = millis * 1000;

                        int nanos = ts.getNanos();
                        long extraMicros = (nanos % 1000000) / 1000;
                        long totalMicros = microseconds + extraMicros;

                        out.writeLong(totalMicros);
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
}
