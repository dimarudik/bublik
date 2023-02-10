package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Chunk {
    private Integer chunkId;
    private String startRowId;
    private String endRowId;
    private Long startId;
    private Long endId;
}
