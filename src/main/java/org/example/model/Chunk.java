package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.example.service.ChunkService;

@NoArgsConstructor
@AllArgsConstructor
@Getter
public abstract class Chunk<T> implements ChunkService {
    private Integer id;
    private T start;
    private T end;
    private Config config;
    private Table sourceTable;
}
