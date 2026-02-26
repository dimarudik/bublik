package dev.bublik.core.model;

import java.util.List;

public record Table2Table<S extends AutoCloseable> (Table<S> sourceTable,
                                                    Table<S> targetTable,
                                                    List<Column2Column> column2Columns,
                                                    Column ttlColumn,
                                                    Column timestampColumn) {
}
