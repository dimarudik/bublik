package org.example.service;

import org.example.model.ChunkDeprecated;

public interface LogMessageService {
    void saveToLogger(ChunkDeprecated chunkDeprecated, int recordCount, long start, String operation);
}
