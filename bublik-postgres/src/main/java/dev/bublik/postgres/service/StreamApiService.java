package dev.bublik.postgres.service;

import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import dev.bublik.core.model.Chunk;
import dev.bublik.core.model.Column;
import dev.bublik.core.model.Column2Column;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.SQLException;

import static dev.bublik.core.util.Utils.getStackTrace;

public class StreamApiService {

    public SimpleRowWriter getSimpleRowWriter(Connection connection,
                                              Chunk<?, ?, ?, ?> chunk,
                                              String schemaName,
                                              String tableName) throws SQLException {
        String[] columnNames = chunk.getT2t().column2Columns()
                .stream()
                .map(Column2Column::targetColumn)
                .map(Column::columnName)
                .toArray(String[]::new);
        SimpleRowWriter.Table table = new SimpleRowWriter.Table(schemaName, tableName, columnNames);
        PGConnection pgConnection = connection.unwrap(PGConnection.class);
        return new SimpleRowWriter(table, pgConnection);
    }

    public void closeSimpleRowWriter(SimpleRowWriter simpleRowWriter) {
        try {
            simpleRowWriter.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
