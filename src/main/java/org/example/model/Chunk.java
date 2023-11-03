package org.example.model;

//@Data
//@AllArgsConstructor
public class Chunk {
    private Integer chunkId;
    private String startRowId;
    private String endRowId;
    private Long startId;
    private Long endId;

    public Chunk(Integer chunkId, String startRowId, String endRowId, Long startId, Long endId) {
        this.chunkId = chunkId;
        this.startRowId = startRowId;
        this.endRowId = endRowId;
        this.startId = startId;
        this.endId = endId;
    }

    public Integer getChunkId() {
        return chunkId;
    }

    public void setChunkId(Integer chunkId) {
        this.chunkId = chunkId;
    }

    public String getStartRowId() {
        return startRowId;
    }

    public void setStartRowId(String startRowId) {
        this.startRowId = startRowId;
    }

    public String getEndRowId() {
        return endRowId;
    }

    public void setEndRowId(String endRowId) {
        this.endRowId = endRowId;
    }

    public Long getStartId() {
        return startId;
    }

    public void setStartId(Long startId) {
        this.startId = startId;
    }

    public Long getEndId() {
        return endId;
    }

    public void setEndId(Long endId) {
        this.endId = endId;
    }
}
