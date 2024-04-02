package org.example.util;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class TableUtil {

    private static final AtomicInteger count = new AtomicInteger(0);
    private static final AtomicLong duration = new AtomicLong(0L);

    private static final Set<String> tableExistsCache = ConcurrentHashMap.newKeySet();

    public static boolean tableExists(Connection connection,
                                      String schemaName,
                                      String tableName) throws SQLException {
        var curVal = count.getAndIncrement();
        if (curVal != 0 && curVal % 100 == 0) {
            printStats(curVal, duration.get());
        }

        var start = System.currentTimeMillis();
        if (tableExistsCache.contains(tableName)) {
            return true;
        }

        ResultSet tablesLowCase = connection.getMetaData().getTables(
                null,
                schemaName.toLowerCase(),
                tableName.toLowerCase(),
                null);
        ResultSet tablesUpCase = connection.getMetaData().getTables(
                null,
                schemaName,
                tableName,
                null);
        if (!tablesLowCase.next() && !tablesUpCase.next()) {
            tablesLowCase.close();
            tablesUpCase.close();
            throw new SQLException("Table " + schemaName + "."
                    + tableName + " does not exist.");
        }
        tablesLowCase.close();
        tablesUpCase.close();
        tableExistsCache.add(tableName);
        var end = System.currentTimeMillis();
        var d = end - start;
        duration.addAndGet(d);
        return true;
    }

    private static void printStats(int curVal, long duration) {
        log.trace("\n\n----STATS---\nCOUNT: {}\nDURATION: {}\n\n", curVal, duration);
    }
}
