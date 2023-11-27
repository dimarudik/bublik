package org.example.model;

public record Chunk(Integer chunkId, String startRowId, String endRowId, Long startId, Long endId) {}
