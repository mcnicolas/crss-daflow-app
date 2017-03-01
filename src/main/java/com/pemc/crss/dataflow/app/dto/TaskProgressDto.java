package com.pemc.crss.dataflow.app.dto;

public class TaskProgressDto {

    private String runningStep;
    private Long executedCount;
    private Long totalCount;

    public String getRunningStep() {
        return runningStep;
    }

    public void setRunningStep(String runningStep) {
        this.runningStep = runningStep;
    }

    public Long getExecutedCount() {
        return executedCount == null ? 0 : executedCount;
    }

    public void setExecutedCount(Long executedCount) {
        this.executedCount = executedCount;
    }

    public Long getTotalCount() {
        return totalCount == null ? 0 : totalCount;
    }

    public void setTotalCount(Long totalCount) {
        this.totalCount = totalCount;
    }
}
