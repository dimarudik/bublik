package org.example.model;

public record OraChunk(Integer chunkId,
                       String startRowId,
                       String endRowId,
                       Long startId,
                       Long endId,
                       Config config) implements Chunk {

    @Override
    public Long startPage() {
        return null;
    }

    @Override
    public Long endPage() {
        return null;
    }
}
