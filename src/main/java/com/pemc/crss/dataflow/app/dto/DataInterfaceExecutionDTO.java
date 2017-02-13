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
    private String mode;
    private String type;
    private String tradingDay;
    private String dispatchInterval;
    private int recordsExpected;
    private int recordsWritten;
    private int recordsRead;
    private int abnormalPrice;

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

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTradingDay() {
        return tradingDay;
    }

    public void setTradingDay(String tradingDay) {
        this.tradingDay = tradingDay;
    }

    public String getDispatchInterval() {
        return dispatchInterval;
    }

    public void setDispatchInterval(String dispatchInterval) {
        this.dispatchInterval = dispatchInterval;
    }

    public int getRecordsExpected() {
        return recordsExpected;
    }

    public void setRecordsExpected(int recordsExpected) {
        this.recordsExpected = recordsExpected;
    }

    public int getRecordsWritten() {
        return recordsWritten;
    }

    public void setRecordsWritten(int recordsWritten) {
        this.recordsWritten = recordsWritten;
    }

    public int getRecordsRead() {
        return recordsRead;
    }

    public void setRecordsRead(int recordsRead) {
        this.recordsRead = recordsRead;
    }

    public int getAbnormalPrice() {
        return abnormalPrice;
    }

    public void setAbnormalPrice(int abnormalPrice) {
        this.abnormalPrice = abnormalPrice;
    }
}
