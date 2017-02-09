package com.pemc.crss.dataflow.app.dto;

import com.google.common.collect.Maps;
import org.springframework.batch.core.BatchStatus;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class StlTaskExecutionDto {

    private Long id;
    private Date runDateTime;
    private String exitMessage;
    private Map<String, Object> params;
    private String status;
    private TaskProgressDto progress;
    private BatchStatus calculationStatus;
    private BatchStatus invoiceGenerationStatus;
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

    public TaskProgressDto getProgress() {
        return progress;
    }

    public void setProgress(TaskProgressDto progress) {
        this.progress = progress;
    }

    public BatchStatus getCalculationStatus() {
        return calculationStatus;
    }

    public void setCalculationStatus(BatchStatus calculationStatus) {
        this.calculationStatus = calculationStatus;
    }

    public BatchStatus getInvoiceGenerationStatus() {
        return invoiceGenerationStatus;
    }

    public void setInvoiceGenerationStatus(BatchStatus invoiceGenerationStatus) {
        this.invoiceGenerationStatus = invoiceGenerationStatus;
    }

    public Map<String, List<TaskSummaryDto>> getSummary() {
        return summary;
    }

    public void setSummary(Map<String, List<TaskSummaryDto>> summary) {
        this.summary = summary;
    }
}
