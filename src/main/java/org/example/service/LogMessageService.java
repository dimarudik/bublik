package org.example.service;

import org.example.model.ChunkDerpicated;

public interface LogMessageService {
    void saveToLogger(ChunkDerpicated chunkDerpicated, int recordCount, long start, String operation);
}
