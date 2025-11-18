package org.bublik.cassandra.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.bublik.cassandra.model.CSTable;
import org.bublik.core.model.Column;
import org.bublik.core.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.bublik.cassandra.constants.SQLConstants.SQL_KEY_BY_TYPE;

public interface CSTableService {
    Logger log = LoggerFactory.getLogger(CSTableService.class);

    static List<Column> getKey(CqlSession cqlSession, Table<?> table, String keyType) {
        ResultSet resultSet = cqlSession.execute(
                SQL_KEY_BY_TYPE,
                table.getSchemaName(),
                table.getTableName(),
                keyType
        );
        List<Column> key = new ArrayList<>();
        for (Row row : resultSet) {
            key.add(new Column(
                    row.getInt("position"),
                    row.getString("column_name"),
                    row.getString("type"),
                    null,
                    null,
                    null,
                    null,
                    null,
                    0,
                    null,
                    0,
                    null,
                    false
            ));
        }
        return key;
    }

    static List<Column> getNonStaticColumns(Table<?> table) {
        return table.getColumns().stream().filter(column -> !column.isStatic()).toList();
    }

    static String countRowsInTableQuery(CSTable<?> table) {
        List<Column> pkColumns = table.getPartitionKey();
        Collections.sort(pkColumns);
        String pkColumnsJoined = String.join(", ", pkColumns.stream().map(Column::columnName).toList());
        return "select " +
                pkColumnsJoined +
                " from " +
                table.getSchemaName() + "." +
                table.getTableName() +
                " where " +
                " token ( " +
                pkColumnsJoined +
                ") > ? and token ( " +
                pkColumnsJoined +
                " ) <= ? ";
    }
}
