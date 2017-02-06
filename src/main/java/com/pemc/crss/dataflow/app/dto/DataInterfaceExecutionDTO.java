package com.pemc.crss.dataflow.app.dto;


import org.springframework.batch.core.BatchStatus;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class DataInterfaceExecutionDTO {

    private Long id;
    private Date runStartDateTime;
    private Date runEndDateTime;
    private List<String> failureExceptions;
    private Map<String, Object> params;
    private String status;
    private TaskProgressDto progress;
    private BatchStatus batchStatus;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getRunStartDateTime() {
        return runStartDateTime;
    }

    public void setRunStartDateTime(Date runStartDateTime) {
        this.runStartDateTime = runStartDateTime;
    }

    public Date getRunEndDateTime() {
        return runEndDateTime;
    }

    public void setRunEndDateTime(Date runEndDateTime) {
        this.runEndDateTime = runEndDateTime;
    }

    public List<String> getFailureExceptions() {
        return failureExceptions;
    }

    public void setFailureExceptions(List<String> failureExceptions) {
        this.failureExceptions = failureExceptions;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public TaskProgressDto getProgress() {
        return progress;
    }

    public void setProgress(TaskProgressDto progress) {
        this.progress = progress;
    }

    public BatchStatus getBatchStatus() {
        return batchStatus;
    }

    public void setBatchStatus(BatchStatus batchStatus) {
        this.batchStatus = batchStatus;
    }
}
