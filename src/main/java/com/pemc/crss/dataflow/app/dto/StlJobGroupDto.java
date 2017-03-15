package com.pemc.crss.dataflow.app.dto;

import org.springframework.batch.core.BatchStatus;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;

public class StlJobGroupDto {

    private BatchStatus stlAmtTaggingStatus;
    private BatchStatus gmrVatMFeeCalculationStatus;
    private BatchStatus gmrVatMFeeTaggingStatus;
    private BatchStatus invoiceGenerationStatus;
    private String status;
    private boolean currentlyRunning;
    private boolean latestAdjustment;
    private boolean header;
    private Long groupId;
    private Date runStartDateTime;
    private Date runEndDateTime;

    private List<PartialCalculationDto> partialCalculationDtos;

    private SortedSet<LocalDate> remainingDates;

    public BatchStatus getStlAmtTaggingStatus() {
        return stlAmtTaggingStatus;
    }

    public void setStlAmtTaggingStatus(BatchStatus stlAmtTaggingStatus) {
        this.stlAmtTaggingStatus = stlAmtTaggingStatus;
    }

    public BatchStatus getGmrVatMFeeCalculationStatus() {
        return gmrVatMFeeCalculationStatus;
    }

    public void setGmrVatMFeeCalculationStatus(BatchStatus gmrVatMFeeCalculationStatus) {
        this.gmrVatMFeeCalculationStatus = gmrVatMFeeCalculationStatus;
    }

    public BatchStatus getGmrVatMFeeTaggingStatus() {
        return gmrVatMFeeTaggingStatus;
    }

    public void setGmrVatMFeeTaggingStatus(BatchStatus gmrVatMFeeTaggingStatus) {
        this.gmrVatMFeeTaggingStatus = gmrVatMFeeTaggingStatus;
    }

    public BatchStatus getInvoiceGenerationStatus() {
        return invoiceGenerationStatus;
    }

    public void setInvoiceGenerationStatus(BatchStatus invoiceGenerationStatus) {
        this.invoiceGenerationStatus = invoiceGenerationStatus;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public boolean isCurrentlyRunning() {
        return currentlyRunning;
    }

    public void setCurrentlyRunning(boolean currentlyRunning) {
        this.currentlyRunning = currentlyRunning;
    }

    public boolean isLatestAdjustment() {
        return latestAdjustment;
    }

    public void setLatestAdjustment(boolean latestAdjustment) {
        this.latestAdjustment = latestAdjustment;
    }

    public List<PartialCalculationDto> getPartialCalculationDtos() {
        return partialCalculationDtos;
    }

    public void setPartialCalculationDtos(List<PartialCalculationDto> partialCalculationDtos) {
        this.partialCalculationDtos = partialCalculationDtos;
    }

    public boolean isHeader() {
        return header;
    }

    public void setHeader(boolean header) {
        this.header = header;
    }

    public SortedSet<LocalDate> getRemainingDates() {
        return remainingDates;
    }

    public void setRemainingDates(SortedSet<LocalDate> remainingDates) {
        this.remainingDates = remainingDates;
    }

    public Long getGroupId() {
        return groupId;
    }

    public void setGroupId(Long groupId) {
        this.groupId = groupId;
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
}
