package org.bublik.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ColumnUtil {
    private static final Logger log = LoggerFactory.getLogger(ColumnUtil.class);


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

    public static Map<String, String> parseHstoreString(String hstoreStr) {
        Map<String, String> result = new HashMap<>();
        if (hstoreStr == null || hstoreStr.isEmpty()) {
            return result;
        }

        // Разделяем по запятым, но учитываем, что значения могут быть в кавычках
        Pattern pairPattern = Pattern.compile("(\"[^\"]+\"|[^=>]+)=>(\"[^\"]+\"|[^,]+)");
        Matcher matcher = pairPattern.matcher(hstoreStr);

        while (matcher.find()) {
            String key = matcher.group(1).replaceAll(", ", "").replaceAll("\"", "").trim();//.replaceAll("^\"|\"$", "").trim();
            String value = matcher.group(2).replaceAll("\"", "");//.replaceAll("^\"|\"$", "").trim();
            result.put(key, value);
        }

        return result;
    }
}
