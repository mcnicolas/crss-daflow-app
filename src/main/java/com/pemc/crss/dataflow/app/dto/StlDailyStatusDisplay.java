package com.pemc.crss.dataflow.app.dto;


import com.pemc.crss.shared.core.dataflow.entity.ViewStlDailyStatus;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;

import java.time.LocalDateTime;

public class StlDailyStatusDisplay {

    private final ViewStlDailyStatus dailyStatus;

    public StlDailyStatusDisplay(ViewStlDailyStatus dailyStatus) {
        this.dailyStatus = dailyStatus;
    }

    public Long getRunId() {
        return dailyStatus.getRunId();
    }

    public LocalDateTime getJobExecStart() {
        return dailyStatus.getJobExecStart();
    }

    public LocalDateTime getJobExecEnd() {
        return dailyStatus.getJobExecEnd();
    }

    public LocalDateTime getTradingDate() {
        return dailyStatus.getTradingDate();
    }

    public JobProcess jobProcess() {
        return dailyStatus.getJobProcess();
    }

    public String getJobProcessLabel() {
        switch (dailyStatus.getJobProcess()) {
            case GEN_INPUT_WS_TA:
                return "Generate Input Workspace";
            case CALC_TA:
                return JobProcess.CALC_TA.getDescription();
            default:
                return null;
        }
    }

    public String getStatus() {
        switch (dailyStatus.getStatus()) {
            case ON_QUEUE:
            case STARTING:
                return "Queued";
            case STARTED:
                return "In Progress";
            case COMPLETED:
                return "Completed";
            case FAILED_EXECUTION:
            case FAILED_RUN:
                return "Failed";
            default:
                return null;
        }
    }

    public String getErrorDetails() {
        switch (dailyStatus.getStatus()) {
            case FAILED_EXECUTION:
                return dailyStatus.getDetails(); // failed execution always has details.
            case FAILED_RUN: // error logs can only be checked by searching for the job in mesos logs. good luck.
                return "Job failed during run.";
            default:
                return null;
        }
    }

    public boolean isCompletedGenIws() {
        return dailyStatus.getStatus() == QueueStatus.COMPLETED
                && dailyStatus.getJobProcess() == JobProcess.GEN_INPUT_WS_TA;
    }
}
