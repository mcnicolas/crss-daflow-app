package com.pemc.crss.dataflow.app.dto;

import org.hibernate.engine.jdbc.batch.spi.Batch;
import org.springframework.batch.core.BatchStatus;

import java.util.List;
import java.util.Map;

public class TaskExecutionDto extends BaseTaskExecutionDto {

    private BatchStatus wesmStatus;
    private BatchStatus rcoaStatus;
    private BatchStatus mqReportStatus;
    private BatchStatus gesqReportStatus;
    private BatchStatus settlementStatus;
    private BatchStatus settlementReadyStatus;
    private BatchStatus mqReportStatusAfterFinalized;
    private BatchStatus gesqReportStatusAfterFinalized;
    private BatchStatus calculationStatus;
    private BatchStatus taggingStatus;
    private BatchStatus stlProcessFinalizedStatus;

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

    public BatchStatus getGesqReportStatus() {
        return gesqReportStatus;
    }

    public void setGesqReportStatus(BatchStatus gesqReportStatus) {
        this.gesqReportStatus = gesqReportStatus;
    }

    public BatchStatus getSettlementStatus() {
        return settlementStatus;
    }

    public void setSettlementStatus(BatchStatus settlementStatus) {
        this.settlementStatus = settlementStatus;
    }

    public BatchStatus getSettlementReadyStatus() {
        return settlementReadyStatus;
    }

    public void setSettlementReadyStatus(BatchStatus settlementReadyStatus) {
        this.settlementReadyStatus = settlementReadyStatus;
    }

    public BatchStatus getMqReportStatusAfterFinalized() {
        return mqReportStatusAfterFinalized;
    }

    public void setMqReportStatusAfterFinalized(BatchStatus mqReportStatusAfterFinalized) {
        this.mqReportStatusAfterFinalized = mqReportStatusAfterFinalized;
    }

    public BatchStatus getGesqReportStatusAfterFinalized() {
        return gesqReportStatusAfterFinalized;
    }

    public void setGesqReportStatusAfterFinalized(BatchStatus gesqReportStatusAfterFinalized) {
        this.gesqReportStatusAfterFinalized = gesqReportStatusAfterFinalized;
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

    public BatchStatus getStlProcessFinalizedStatus() {
        return stlProcessFinalizedStatus;
    }

    public void setStlProcessFinalizedStatus(BatchStatus stlProcessFinalizedStatus) {
        this.stlProcessFinalizedStatus = stlProcessFinalizedStatus;
    }
}
