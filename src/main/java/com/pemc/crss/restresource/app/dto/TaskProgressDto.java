package com.pemc.crss.restresource.app.dto;

public class TaskProgressDto {

    private String runningStep;
    private long executedCount;
    private long totalCount;

    public String getRunningStep() {
        return runningStep;
    }

    public void setRunningStep(String runningStep) {
        this.runningStep = runningStep;
    }

    public long getExecutedCount() {
        return executedCount;
    }

    public void setExecutedCount(long executedCount) {
        this.executedCount = executedCount;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }
}
