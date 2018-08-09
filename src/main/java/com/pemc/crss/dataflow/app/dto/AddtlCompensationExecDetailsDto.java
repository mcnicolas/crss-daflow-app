package com.pemc.crss.dataflow.app.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AddtlCompensationExecDetailsDto {
    private Long runId;
    private String billingId;
    private String mtn;
    private BigDecimal approvedRate;
    private String status;
    private LocalDateTime runStartDate;
    private LocalDateTime runEndDate;
    private List<TaskSummaryDto> taskSummaryList = new ArrayList<>();

    // progress bar and status
    private List<String> runningSteps = new ArrayList<>();

    public Long getRunId() {
        return runId;
    }

    public void setRunId(Long runId) {
        this.runId = runId;
    }

    public String getBillingId() {
        return billingId;
    }

    public void setBillingId(String billingId) {
        this.billingId = billingId;
    }

    public String getMtn() {
        return mtn;
    }

    public void setMtn(String mtn) {
        this.mtn = mtn;
    }

    public BigDecimal getApprovedRate() {
        return approvedRate;
    }

    public void setApprovedRate(BigDecimal approvedRate) {
        this.approvedRate = approvedRate;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getRunStartDate() {
        return runStartDate;
    }

    public void setRunStartDate(LocalDateTime runStartDate) {
        this.runStartDate = runStartDate;
    }

    public LocalDateTime getRunEndDate() {
        return runEndDate;
    }

    public void setRunEndDate(LocalDateTime runEndDate) {
        this.runEndDate = runEndDate;
    }

    public List<TaskSummaryDto> getTaskSummaryList() {
        return taskSummaryList;
    }

    public void setTaskSummaryList(List<TaskSummaryDto> taskSummaryList) {
        this.taskSummaryList = taskSummaryList;
    }

    public List<String> getRunningSteps() {
        return runningSteps;
    }

    public void setRunningSteps(List<String> runningSteps) {
        this.runningSteps = runningSteps;
    }
}
