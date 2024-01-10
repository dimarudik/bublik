package org.example.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TableUtil {
    public static boolean tableExists(Connection connection,
                                      String schemaName,
                                      String tableName) throws SQLException {
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
        return true;
    }
}
