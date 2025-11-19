package org.bublik.cassandra.storage.cassandraaddons;

import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import org.bublik.core.constants.PGKeywords;
import org.bublik.core.model.Chunk;
import org.bublik.core.model.Column;
import org.bublik.core.model.Table2Table;

import java.util.List;

public record CSRecord (TokenRange tokenRange,
                        List<CSValue> values) {

    public String buildInsertStatement(Chunk<? ,?, ?, ?> chunk) {
        Table2Table<?> t2t = chunk.getT2t();
//        List<Column> columns = values.stream().map(CSValue::column).toList();
        List<String> columnNames = values.stream().map(CSValue::column).map(Column::columnName).toList();
        StringBuilder usingClause = new StringBuilder();
        if (t2t.ttlColumn() != null || t2t.timestampColumn() != null) {
            usingClause.append(" using ");
        }
        if (t2t.ttlColumn() != null && t2t.timestampColumn() != null) {
            usingClause.append(" ttl ").append(" :ttl and ");
        } else if (t2t.ttlColumn() != null) {
            usingClause.append(" ttl ").append(" :ttl ");
        }
        if (t2t.timestampColumn() != null) {
            usingClause.append(" timestamp ").append(" :timestamp ");
        }
        return PGKeywords.INSERT + " " + PGKeywords.INTO + " " +
                chunk.getConfig().toSchemaName() + "." +
                chunk.getConfig().toTableName() + " (" +
                String.join(", ",  columnNames) + ") " +
                PGKeywords.VALUES + " (" + " :" +
                String.join(", :", columnNames) +
                ")" +
                usingClause +
                ";";
    }
}
