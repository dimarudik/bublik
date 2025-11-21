package org.bublik.cassandra.storage.cassandraaddons;

import org.bublik.core.model.Column;

import java.util.function.Function;

public record CSValue(Column column, Object value, CSValueAttribute attribute) {

    @Override
    public String toString() {
        return "{" + column.columnName() + " : " + value +
                ", " + attribute.ttl() +
                ", " + attribute.timestamp() +
                '}';
    }

    public boolean isRegular() {
        return column().columnPosition() == -1 && !column().isStatic();
    }

    public boolean isNonRegular() {
        return column().columnPosition() != -1 || column().isStatic();
    }

    public boolean isEmpty() {
        return value == null && attribute.isEmpty();
    }

    public boolean isNotEmpty() {
        return !isEmpty();
    }

    public CSValueAttribute groupByAttribute() {
        Function<CSValue,CSValueAttribute> f = v -> v.attribute().isEmpty() ? new CSValueAttribute(0, 0L) : v.attribute();
        return f.apply(this);
    }
}
