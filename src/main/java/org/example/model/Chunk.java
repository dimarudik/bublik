package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.service.ChunkService;

@NoArgsConstructor
@AllArgsConstructor
@Data
public abstract class Chunk<T> implements ChunkService {
    private Integer id;
    private T start;
    private T end;
    private Config config;
}
