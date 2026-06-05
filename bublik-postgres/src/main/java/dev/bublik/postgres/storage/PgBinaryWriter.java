package dev.bublik.postgres.storage;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PgBinaryWriter implements AutoCloseable {
    private final DataOutputStream out;
    private static final LocalDate PG_EPOCH_DATE = LocalDate.of(2000, 1, 1);
    private static final LocalDateTime PG_EPOCH_DATETIME = LocalDateTime.of(2000, 1, 1, 0, 0, 0);

    private static final byte[] BINARY_HEADER = new byte[]{
            'P','G','C','O','P','Y','\n', (byte) 255, '\r', '\n', 0,
            0, 0, 0, 0,
            0, 0, 0, 0
    };

    public PgBinaryWriter(OutputStream os, int javaBufferSize) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(os, javaBufferSize);
        this.out = new DataOutputStream(bos);
        this.out.write(BINARY_HEADER);
    }

    public DataOutputStream getOut() {
        return out;
    }

    public void startRow(short columnCount) throws IOException {
        out.writeShort(columnCount);
    }

    public void writeNull() throws IOException {
        out.writeInt(-1);
    }

    public void writeInt(Integer value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(4);
            out.writeInt(value);
        }
    }

    public void writeString(String value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
        } else {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    public void writeNumeric(BigDecimal value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
            return;
        }

        // 1. Получаем масштаб (количество знаков после запятой)
        short dscale = (short) value.scale();

        // 2. Определяем знак (0x0000 - плюс, 0x4000 - минус)
        short sign = (short) (value.signum() >= 0 ? 0x0000 : 0x4000);

        // 3. Переводим число в строку без экспонент, чтобы четко разобрать целую и дробную части
        String plainString = value.abs().toPlainString();

        // Разбиваем строку по точке на целое и дробь
        int dotIndex = plainString.indexOf('.');
        String intPart = dotIndex < 0 ? plainString : plainString.substring(0, dotIndex);
        String fracPart = dotIndex < 0 ? "" : plainString.substring(dotIndex + 1);

        // Дополняем целую часть нулями слева, чтобы длина делилась на 4
        while (intPart.length() % 4 != 0) {
            intPart = "0" + intPart;
        }
        // Дополняем дробную часть нулями справа, чтобы длина делилась на 4
        while (fracPart.length() % 4 != 0) {
            fracPart = fracPart + "0";
        }

        List<Short> digits = new ArrayList<>();

        // Собираем 10000-ичные разряды целой части
        for (int i = 0; i < intPart.length(); i += 4) {
            digits.add(Short.parseShort(intPart.substring(i, i + 4)));
        }

        // Вес (weight) — это количество групп целой части минус 1
        short weight = (short) (digits.size() - 1);

        // Собираем 10000-ичные разряды дробной части
        for (int i = 0; i < fracPart.length(); i += 4) {
            digits.add(Short.parseShort(fracPart.substring(i, i + 4)));
        }

        // Удаляем лишние нули в начале (если число меньше единицы, например 0.12)
        while (digits.size() > 0 && digits.get(0) == 0) {
            digits.remove(0);
        }

        // Если число было ровно 0
        if (digits.isEmpty()) {
            digits.add((short) 0);
            weight = 0;
        }

        // Полный размер пакета данных: 8 байт заголовка + по 2 байта на каждый разряд
        int dataLength = 8 + (digits.size() * 2);

        // Записываем структуру в бинарный поток COPY
        out.writeInt(dataLength);          // Длина всего поля
        out.writeShort(digits.size());     // Количество 10000-ичных цифр
        out.writeShort(weight);            // Вес старшего разряда
        out.writeShort(sign);              // Знак числа
        out.writeShort(dscale);            // Масштаб (scale)

        // Записываем сами разряды
        for (short digit : digits) {
            out.writeShort(digit);
        }
    }

    private void writeAnyStringArray(String[] array, int elementOid) throws IOException {
        if (array == null) {
            out.writeInt(-1);
            return;
        }

        int hasNulls = 0;
        int totalElementsLength = 0;
        List<byte[]> encodedStrings = new ArrayList<>(array.length);

        for (String s : array) {
            if (s == null) {
                hasNulls = 1;
                totalElementsLength += 4;
                encodedStrings.add(null);
            } else {
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                totalElementsLength += 4 + bytes.length;
                encodedStrings.add(bytes);
            }
        }

        int dataLength = 20 + totalElementsLength;

        out.writeInt(dataLength);
        out.writeInt(1);
        out.writeInt(hasNulls);
        out.writeInt(elementOid);
        out.writeInt(array.length);
        out.writeInt(1);

        for (byte[] bytes : encodedStrings) {
            if (bytes == null) {
                out.writeInt(-1);
            } else {
                out.writeInt(bytes.length);
                out.write(bytes);
            }
        }
    }

    public void writeVarcharArray(String[] array) throws IOException {
        writeAnyStringArray(array, 1043);
    }

    public void writeTextArray(String[] array) throws IOException {
        writeAnyStringArray(array, 25);
    }

    public void writeJsonb(String jsonString) throws IOException {
        if (jsonString == null) {
            out.writeInt(-1);
            return;
        }

        byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);
        int dataLength = 1 + jsonBytes.length;
        out.writeInt(dataLength);
        out.writeByte(1);
        out.write(jsonBytes);
    }

    public void writeShort(Short value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(2);
            out.writeShort(value);
        }
    }

    public void writeLong(Long value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(8);
            out.writeLong(value);
        }
    }

    public void writeFloat(Float value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(4);
            out.writeFloat(value);
        }
    }

    public void writeDouble(Double value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(8);
            out.writeDouble(value);
        }
    }

    public void writeBoolean(Boolean value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(1);
            out.writeByte(value ? 1 : 0);
        }
    }

    public void writeUuid(java.util.UUID uuid) throws IOException {
        if (uuid == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(16);
            out.writeLong(uuid.getMostSignificantBits());
            out.writeLong(uuid.getLeastSignificantBits());
        }
    }

    public void writeDate(LocalDate date) throws IOException {
        if (date == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(4);
            long days = ChronoUnit.DAYS.between(PG_EPOCH_DATE, date);
            out.writeInt((int) days);
        }
    }

    public void writeTimestamp(LocalDateTime dateTime) throws IOException {
        if (dateTime == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(8);
            long micros = ChronoUnit.MICROS.between(PG_EPOCH_DATETIME, dateTime);
            out.writeLong(micros);
        }
    }

    public void writeTimestampTz(OffsetDateTime offsetDateTime) throws IOException {
        if (offsetDateTime == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(8);
            LocalDateTime utcDateTime = offsetDateTime.withOffsetSameInstant(java.time.ZoneOffset.UTC).toLocalDateTime();
            long micros = ChronoUnit.MICROS.between(PG_EPOCH_DATETIME, utcDateTime);
            out.writeLong(micros);
        }
    }

    public void writeTime(LocalTime time) throws IOException {
        if (time == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(8);
            long micros = (time.getHour() * 3600L + time.getMinute() * 60L + time.getSecond()) * 1_000_000L
                    + (time.getNano() / 1_000L);

            out.writeLong(micros);
        }
    }

    public void writeBytea(byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    public void writeInet(InetAddress address) throws IOException {
        if (address == null) {
            out.writeInt(-1);
            return;
        }

        byte[] rawAddress = address.getAddress();
        byte family = (byte) (rawAddress.length == 4 ? 2 : 3);
        byte mask = (byte) (rawAddress.length == 4 ? 32 : 128);
        byte isCidr = 0;
        byte size = (byte) rawAddress.length;

        out.writeInt(4 + size);

        out.writeByte(family);
        out.writeByte(mask);
        out.writeByte(isCidr);
        out.writeByte(size);

        out.write(rawAddress);
    }

    public void writeHstore(Map<String, String> map) throws IOException {
        if (map == null) {
            out.writeInt(-1);
            return;
        }

        int totalBytes = 4;

        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey() == null) continue;

            byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            totalBytes += 4 + keyBytes.length;

            if (entry.getValue() == null) {
                totalBytes += 4;
            } else {
                byte[] valBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
                totalBytes += 4 + valBytes.length;
            }
        }

        out.writeInt(totalBytes);
        out.writeInt(map.size());

        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey() == null) continue;

            byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            out.writeInt(keyBytes.length);
            out.write(keyBytes);

            if (entry.getValue() == null) {
                out.writeInt(-1);
            } else {
                byte[] valBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
                out.writeInt(valBytes.length);
                out.write(valBytes);
            }
        }
    }

    public void writeLongArray(Long[] array) throws IOException {
        if (array == null) {
            out.writeInt(-1);
            return;
        }

        final int BIGINT_ELEMENT_OID = 20; // OID типа bigint в PostgreSQL
        int hasNulls = 0;
        int totalElementsLength = 0;

        for (Long val : array) {
            if (val == null) {
                hasNulls = 1;
                totalElementsLength += 4; // 4 байта под маркер длины -1
            } else {
                totalElementsLength += 4 + 8; // 4 байта длины + 8 байт самого Long
            }
        }

        // 20 байт заголовка + длина элементов
        int dataLength = 20 + totalElementsLength;

        out.writeInt(dataLength);
        out.writeInt(1);                    // ndims: одномерный массив
        out.writeInt(hasNulls);             // has_nulls: флаг наличия null элементов
        out.writeInt(BIGINT_ELEMENT_OID);   // element_type: OID типа bigint
        out.writeInt(array.length);         // dim: количество элементов
        out.writeInt(1);                    // lbound: нижняя граница индекса в Postgres

        for (Long val : array) {
            if (val == null) {
                out.writeInt(-1);
            } else {
                out.writeInt(8); // длина элемента — всегда 8 байт
                out.writeLong(val);
            }
        }
    }

    public void writeUuidArray(UUID[] array) throws IOException {
        if (array == null) {
            out.writeInt(-1);
            return;
        }

        final int UUID_ELEMENT_OID = 2950; // OID типа uuid в PostgreSQL
        int hasNulls = 0;
        int totalElementsLength = 0;

        for (UUID uuid : array) {
            if (uuid == null) {
                hasNulls = 1;
                totalElementsLength += 4; // 4 байта под маркер длины -1
            } else {
                totalElementsLength += 4 + 16; // 4 байта длины + 16 байт структуры UUID
            }
        }

        int dataLength = 20 + totalElementsLength;

        out.writeInt(dataLength);
        out.writeInt(1);                    // ndims
        out.writeInt(hasNulls);             // has_nulls
        out.writeInt(UUID_ELEMENT_OID);     // element_type: OID типа uuid
        out.writeInt(array.length);         // dim
        out.writeInt(1);                    // lbound

        for (UUID uuid : array) {
            if (uuid == null) {
                out.writeInt(-1);
            } else {
                out.writeInt(16); // длина элемента — всегда 16 байт
                out.writeLong(uuid.getMostSignificantBits());
                out.writeLong(uuid.getLeastSignificantBits());
            }
        }
    }

    public void writeTstzRange(ZonedDateTime lower, boolean lowerInclusive,
                               ZonedDateTime upper, boolean upperInclusive) throws IOException {
        int flags = 0; // Используем int для маски флагов
        int dataLength = 1; // 1 байт под сам флаг

        // Настраиваем флаги нижней границы
        if (lower == null) {
            flags |= 0x08; // Нижная граница бесконечна
        } else {
            if (lowerInclusive) flags |= 0x02; // Включена '['
            dataLength += 4 + 8; // 4 байта длины поля + 8 байт значения
        }

        // Настраиваем флаги верхней границы
        if (upper == null) {
            flags |= 0x10; // Верхняя граница бесконечна
        } else {
            if (upperInclusive) flags |= 0x04; // Включена ']'
            dataLength += 4 + 8; // 4 байта длины поля + 8 байт значения
        }

        // 1. Пишем общую длину поля диапазона
        out.writeInt(dataLength);

        // 2. Пишем 1 байт флагов границы (метод DataOutputStream принимает int, но пишет 1 байт)
        out.writeByte(flags);

        // 3. Пишем нижнюю границу (если она не бесконечна)
        if (lower != null) {
            out.writeInt(8); // Длина значения — всегда 8 байт (int64)
            LocalDateTime lowerUtc = lower.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
            long micros = ChronoUnit.MICROS.between(PG_EPOCH_DATETIME, lowerUtc);
            out.writeLong(micros);
        }

        // 4. Пишем верхнюю границу (если она не бесконечна)
        if (upper != null) {
            out.writeInt(8); // Длина значения — всегда 8 байт (int64)
            LocalDateTime upperUtc = upper.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
            long micros = ChronoUnit.MICROS.between(PG_EPOCH_DATETIME, upperUtc);
            out.writeLong(micros);
        }
    }

    public void writeEmptyRange() throws IOException {
        out.writeInt(1);
        out.writeByte(1);
    }

    public void writeInterval(int months, int days, long microseconds) throws IOException {
        out.writeInt(16);
        out.writeLong(microseconds);
        out.writeInt(days);
        out.writeInt(months);
    }

    @Override
    public void close() throws IOException {
        try {
            out.writeShort(-1);
            out.flush();
        } finally {
            out.close();
        }
    }
}
