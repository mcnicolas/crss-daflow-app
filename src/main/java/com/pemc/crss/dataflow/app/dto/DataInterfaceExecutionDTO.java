package com.pemc.crss.dataflow.app.dto;


import org.springframework.batch.core.BatchStatus;
import org.springframework.scheduling.config.Task;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class DataInterfaceExecutionDTO extends TaskExecutionDto {

    private Long id;
    private Date runStartDateTime;
    private Date runEndDateTime;
    private Date tradingDayStart;
    private Date tradingDayEnd;
    private List<String> failureExceptions;
    private Map<String, Object> params;
    private String status;
    private TaskProgressDto progress;
    private BatchStatus batchStatus;
    private String mode;
    private String type;
    private int recordsWritten;
    private int recordsRead;
    private int expectedRecord;

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

    public Date getTradingDayStart() {
        return tradingDayStart;
    }

    public void setTradingDayStart(Date tradingDayStart) {
        this.tradingDayStart = tradingDayStart;
    }

    public Date getTradingDayEnd() {
        return tradingDayEnd;
    }

    public void setTradingDayEnd(Date tradingDayEnd) {
        this.tradingDayEnd = tradingDayEnd;
    }

    public int getExpectedRecord() {
        return expectedRecord;
    }

    public void setExpectedRecord(int expectedRecord) {
        this.expectedRecord = expectedRecord;
    }
}
