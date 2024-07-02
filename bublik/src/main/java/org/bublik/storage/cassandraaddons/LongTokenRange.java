package org.bublik.storage.cassandraaddons;

public class LongTokenRange {
    private final long start;
    private final long end;

    LongTokenRange(Object start, Object end) {
        this.start = (long) start;
        this.end = (long) end;
    }

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    boolean contains(long token) {
        return token >= start && token <= end;
    }
}
