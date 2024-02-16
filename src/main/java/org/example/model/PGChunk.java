package org.example.model;

public record PGChunk(Integer chunkId,
                      Long startPage,
                      Long endPage,
                      Config config) implements Chunk {

    @Override
    public String startRowId() {
        return this.startPage().toString();
    }

    @Override
    public String endRowId() {
        return this.endPage().toString();
    }

    @Override
    public Long startId() {
        return null;
    }

    @Override
    public Long endId() {
        return null;
    }
}
