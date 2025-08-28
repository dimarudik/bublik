package org.bublik.postgres.util;

import de.bytefish.pgbulkinsert.pgsql.model.interval.Interval;
import org.postgresql.replication.LogSequenceNumber;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.Byte.toUnsignedInt;
import static org.bublik.core.constants.SQLConstants.SQL_PG_CURRENT_LSN_AND_XID;

public class ColumnUtil {
    private static final int HIGH_BIT_FLAG = 0x80000000;

    public static Interval byteArrayYMToInterval(byte[] bytes) {
        int year = toUnsignedInt(bytes[0]) << 24
                | toUnsignedInt(bytes[1]) << 16
                | toUnsignedInt(bytes[2]) << 8
                | toUnsignedInt(bytes[3]);
        year ^= HIGH_BIT_FLAG;
        int month = toUnsignedInt(bytes[4]) - 60;
        return new Interval(year * 12 + month, 0,0);
    }

    public static Interval byteArrayDSToInterval(byte[] bytes) {
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

    public static LogSequenceNumber getCurrentLSN(Connection sqlConnection) throws SQLException {
        try (Statement st = sqlConnection.createStatement();
             ResultSet rs = st.executeQuery(SQL_PG_CURRENT_LSN_AND_XID)) {
            if (rs.next()) {
                String lsn = rs.getString(1);
                System.out.println(lsn);
                return LogSequenceNumber.valueOf(lsn);
            } else {
                return LogSequenceNumber.INVALID_LSN;
            }
        }
    }
}
