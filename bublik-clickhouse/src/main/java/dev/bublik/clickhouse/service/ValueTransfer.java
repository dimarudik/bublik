package dev.bublik.clickhouse.service;

import com.google.common.io.LittleEndianDataOutputStream;

@FunctionalInterface
public interface ValueTransfer {
    void transfer(Object v, LittleEndianDataOutputStream out) throws Exception;
}
