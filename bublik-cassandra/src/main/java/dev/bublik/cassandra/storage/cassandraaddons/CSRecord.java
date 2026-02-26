package dev.bublik.cassandra.storage.cassandraaddons;

import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import dev.bublik.core.constants.PGKeywords;
import dev.bublik.core.model.Chunk;
import dev.bublik.core.model.Column;
import dev.bublik.core.model.Table2Table;

import java.util.List;

public record CSRecord (TokenRange tokenRange,
                        List<CSValue> values,
                        CSValueAttribute attribute) {

    public String buildInsertStatement(Chunk<? ,?, ?, ?> chunk) {
        Table2Table<?> t2t = chunk.getT2t();
        List<String> columnNames = values
                .stream()
                .map(CSValue::column)
                .map(Column::columnName)
                .toList();
        StringBuilder usingClause = new StringBuilder();
        if (attribute.ttl() != null || attribute.timestamp() != null) {
            usingClause.append(" using ");
        }
        if (attribute.ttl() != null && attribute.timestamp() != null) {
            usingClause.append(" ttl ").append(" :ttl and ");
        } else if (attribute.ttl() != null) {
            usingClause.append(" ttl ").append(" :ttl ");
        }
        if (attribute.timestamp() != null) {
            usingClause.append(" timestamp ").append(" :timestamp ");
        }
        return PGKeywords.INSERT + " " + PGKeywords.INTO + " " +
                t2t.targetTable().getSchemaName() + "." +
                t2t.targetTable().getTableName() + " (" +
                String.join(", ",  columnNames) + ") " +
                PGKeywords.VALUES + " (" + " :" +
                String.join(", :", columnNames) +
                ")" +
                usingClause +
                ";";
    }

    @Override
    public String toString() {
        return tokenRange + ", " +
                values +
                ", " + attribute;
    }
}
