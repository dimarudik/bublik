package org.example.service;

import org.example.model.Chunk;

public interface LogMessageService {
    void saveToLogger(Chunk<?> chunk, int recordCount, long start, String operation);
}
