package org.example.model;

public record LogMessage (String fromTaskName,
                          String fromTableName,
                          int rowCount,
                          String startRowId,
                          String endRowId,
                          Integer chunkId) {
    @Override
    public String toString() {
        return " of " + rowCount +
                " rows from ROWID " + startRowId +
                " to ROWID " + endRowId +
                " of chunk_id " + chunkId;
    }
}
