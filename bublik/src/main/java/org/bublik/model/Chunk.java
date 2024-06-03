package org.bublik.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.bublik.service.ChunkService;

@AllArgsConstructor
@Getter
public abstract class Chunk<T> implements ChunkService {
    private Integer id;
    private T start;
    private T end;
    private Config config;
    private Table sourceTable;
    private Table targetTable;
}
