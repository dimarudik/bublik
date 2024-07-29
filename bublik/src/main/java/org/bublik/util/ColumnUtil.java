package org.bublik.util;

import de.bytefish.pgbulkinsert.pgsql.model.interval.Interval;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.Byte.toUnsignedInt;

public class ColumnUtil {
    private static final int HIGH_BIT_FLAG = 0x80000000;

    public static Interval intervalYM2Interval(INTERVALYM intervalym) {
        byte[] bytes;
        bytes = intervalym.toBytes();
        int year = toUnsignedInt(bytes[0]) << 24
                | toUnsignedInt(bytes[1]) << 16
                | toUnsignedInt(bytes[2]) << 8
                | toUnsignedInt(bytes[3]);
        year ^= HIGH_BIT_FLAG;
        int month = toUnsignedInt(bytes[4]) - 60;
        return new Interval(year * 12 + month, 0,0);
    }

    public static Interval intervalDS2Interval(INTERVALDS intervalds) {
        byte[] bytes;
        bytes = intervalds.toBytes();
        int day = toUnsignedInt(bytes[0]) << 24
                | toUnsignedInt(bytes[1]) << 16
                | toUnsignedInt(bytes[2]) << 8
                | toUnsignedInt(bytes[3]);
        day ^= HIGH_BIT_FLAG;
        int hour = toUnsignedInt(bytes[4]) - 60;
        int minute = toUnsignedInt(bytes[5]) - 60;
        int second = toUnsignedInt(bytes[6]) - 60;
        int nano = toUnsignedInt(bytes[7]) << 24
                | toUnsignedInt(bytes[8]) << 16
                | toUnsignedInt(bytes[9]) << 8
                | toUnsignedInt(bytes[10]);
        nano ^= HIGH_BIT_FLAG;
        return new Interval(0,
                day,
                hour,
                minute,
                second,
                nano / 1000);
    }

    public static byte[] convertBlobToBytes(ResultSet resultSet, int i) throws SQLException {
        Blob blob = resultSet.getBlob(i);
        return getBlobBytes(blob);
    }

    public static byte[] convertBlobToBytes(ResultSet resultSet, String columnName) throws SQLException {
        Blob blob = resultSet.getBlob(columnName);
        return getBlobBytes(blob);
    }

    private static byte[] getBlobBytes(Blob blob) throws SQLException {
        return blob.getBytes(1, (int) blob.length());
    }

    public static String convertClobToString(ResultSet resultSet, int i) throws SQLException {
        Clob clob = resultSet.getClob(i);
        return getClobString(clob);
    }

    public static String convertClobToString(ResultSet resultSet, String columnName) throws SQLException {
        Clob clob = resultSet.getClob(columnName);
        return getClobString(clob);
    }

    private static String getClobString(Clob clob) throws SQLException {
        return clob.getSubString(1L, (int) clob.length());
    }

    public static int getColumnIndexByColumnName(ResultSet resultSet, String columnName) throws SQLException {
        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            if (columnName.equals(resultSet.getMetaData().getColumnName(i))) {
                return i;
            }
        }
        return 0;
    }
}
