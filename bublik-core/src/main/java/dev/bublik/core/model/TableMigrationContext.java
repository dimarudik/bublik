package dev.bublik.core.model;

public record TableMigrationContext(Config config,
                                    Table2Table t2t,
                                    String chunkLookupSql,
                                    String fetchQuery,
                                    String orderByClause) {
}
