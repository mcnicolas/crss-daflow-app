package com.pemc.crss.dataflow.app.dto;

import org.springframework.batch.core.BatchStatus;

import java.util.List;
import java.util.Map;

public class TaskExecutionDto extends BaseTaskExecutionDto {

    private BatchStatus wesmStatus;
    private BatchStatus rcoaStatus;
    private BatchStatus mqReportStatus;
    private BatchStatus settlementStatus;
    private BatchStatus calculationStatus;
    private BatchStatus taggingStatus;


    public BatchStatus getWesmStatus() {
        return wesmStatus;
    }

    public void setWesmStatus(BatchStatus wesmStatus) {
        this.wesmStatus = wesmStatus;
    }

    public BatchStatus getRcoaStatus() {
        return rcoaStatus;
    }

    public void setRcoaStatus(BatchStatus rcoaStatus) {
        this.rcoaStatus = rcoaStatus;
    }

    public BatchStatus getMqReportStatus() {
        return mqReportStatus;
    }

    public void setMqReportStatus(BatchStatus mqReportStatus) {
        this.mqReportStatus = mqReportStatus;
    }

    public BatchStatus getSettlementStatus() {
        return settlementStatus;
    }

    public void setSettlementStatus(BatchStatus settlementStatus) {
        this.settlementStatus = settlementStatus;
    }

    public BatchStatus getCalculationStatus() {
        return calculationStatus;
    }

    public void setCalculationStatus(BatchStatus calculationStatus) {
        this.calculationStatus = calculationStatus;
    }

    public BatchStatus getTaggingStatus() {
        return taggingStatus;
    }

    public void setTaggingStatus(BatchStatus taggingStatus) {
        this.taggingStatus = taggingStatus;
    }
}
