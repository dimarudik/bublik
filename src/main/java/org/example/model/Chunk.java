package org.example.model;

public interface Chunk {
    Integer chunkId();
    String startRowId();
    String endRowId();
    Long startId();
    Long endId();
    Long startPage();
    Long endPage();
    Config config();
}
