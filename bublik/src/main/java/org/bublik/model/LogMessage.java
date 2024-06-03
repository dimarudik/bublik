package org.bublik.model;

public record LogMessage (
                          int rowCount,
                          long start,
                          String operation,
                          Chunk<?> chunk) {
    @Override
    public String toString() {
        String toTableName = chunk.getTargetTable() == null ? "" : " to " + chunk.getTargetTable().getTableName();
        return  "from " + chunk.getSourceTable().getTableName() +
                toTableName +
                " of " + rowCount +
                " rows (start:" + chunk.getStart() +
                ", end:" + chunk.getEnd() +
                ") chunk_id:" + chunk.getId();
    }
}
