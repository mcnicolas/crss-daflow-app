package com.pemc.crss.dataflow.app.dto;

import com.google.common.collect.Maps;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by on 3/2/17.
 */
public class BaseTaskExecutionDto {

    private Long id;
    private Date runDateTime;
    private String exitMessage;
    private Map<String, Object> params;
    private String status;
    private String statusDetails;
    private TaskProgressDto progress;

    private Map<String, List<TaskSummaryDto>> summary = Maps.newHashMap();

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getRunDateTime() {
        return runDateTime;
    }

    public void setRunDateTime(Date runDateTime) {
        this.runDateTime = runDateTime;
    }

    public String getExitMessage() {
        return exitMessage;
    }

    public void setExitMessage(String exitMessage) {
        this.exitMessage = exitMessage;
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

    public String getStatusDetails() {
        return statusDetails;
    }

    public void setStatusDetails(String statusDetails) {
        this.statusDetails = statusDetails;
    }

    public TaskProgressDto getProgress() {
        return progress;
    }

    public void setProgress(TaskProgressDto progress) {
        this.progress = progress;
    }

    public Map<String, List<TaskSummaryDto>> getSummary() {
        return summary;
    }

    public void setSummary(Map<String, List<TaskSummaryDto>> summary) {
        this.summary = summary;
    }
}
