package com.pemc.crss.dataflow.app.dto;

import com.pemc.crss.shared.commons.util.DateUtil;
import org.springframework.batch.core.BatchStatus;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Collectors;

public class StlJobGroupDto {

    private BatchStatus gmrVatMFeeCalculationStatus;
    private BatchStatus taggingStatus;
    private BatchStatus invoiceGenerationStatus;
    private String status;
    private boolean currentlyRunning;
    private boolean latestAdjustment;
    private boolean header;
    private Long groupId;
    private Date runStartDateTime;
    private Date runEndDateTime;
    private Long runId;

    // progress bar and status
    private Date latestJobExecStartDate;
    private Date latestJobExecEndDate;
    private List<String> runningSteps;

    // folder in sftp server where files are uploaded
    private String invoiceGenFolder;

    private String billingPeriodStr;

    private List<JobCalculationDto> jobCalculationDtos = new ArrayList<>();

    private Map<Long, SortedSet<LocalDate>> remainingDatesMap = new HashMap<>();

    private boolean stlCalculation;

    private Date maxPartialCalcRunDate;

    private Date gmrCalcRunDate;

    public BatchStatus getGmrVatMFeeCalculationStatus() {
        return gmrVatMFeeCalculationStatus;
    }

    public void setGmrVatMFeeCalculationStatus(BatchStatus gmrVatMFeeCalculationStatus) {
        this.gmrVatMFeeCalculationStatus = gmrVatMFeeCalculationStatus;
    }

    public BatchStatus getTaggingStatus() {
        return taggingStatus;
    }

    public void setTaggingStatus(BatchStatus taggingStatus) {
        this.taggingStatus = taggingStatus;
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

    public List<JobCalculationDto> getJobCalculationDtos() {
        return jobCalculationDtos;
    }

    public void setJobCalculationDtos(List<JobCalculationDto> jobCalculationDtos) {
        this.jobCalculationDtos = jobCalculationDtos;
    }

    public boolean isHeader() {
        return header;
    }

    public void setHeader(boolean header) {
        this.header = header;
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

    public String getRunStartDateTimeStr() {
        return runEndDateTime != null
                ? DateUtil.convertToString(runStartDateTime, DateUtil.DEFAULT_DATETIME_FORMAT) : null;
    }

    public void setRunStartDateTime(Date runStartDateTime) {
        this.runStartDateTime = runStartDateTime;
    }

    public Date getRunEndDateTime() {
        return runEndDateTime;
    }

    public String getRunEndDateTimeStr() {
        return runEndDateTime != null
                ? DateUtil.convertToString(runEndDateTime, DateUtil.DEFAULT_DATETIME_FORMAT) : null;
    }

    public void setRunEndDateTime(Date runEndDateTime) {
        this.runEndDateTime = runEndDateTime;
    }

    public Long getRunId() {
        return runId;
    }

    public void setRunId(Long runId) {
        this.runId = runId;
    }

    public String getInvoiceGenFolder() {
        return invoiceGenFolder;
    }

    public void setInvoiceGenFolder(String invoiceGenFolder) {
        this.invoiceGenFolder = invoiceGenFolder;
    }

    public Map<Long, SortedSet<LocalDate>> getRemainingDatesMap() {
        return remainingDatesMap;
    }

    public void setRemainingDatesMap(Map<Long, SortedSet<LocalDate>> remainingDatesMap) {
        this.remainingDatesMap = remainingDatesMap;
    }

    public Date getLatestJobExecStartDate() {
        return latestJobExecStartDate;
    }

    public void setLatestJobExecStartDate(Date latestJobExecStartDate) {
        this.latestJobExecStartDate = latestJobExecStartDate;
    }

    public Date getLatestJobExecEndDate() {
        return latestJobExecEndDate;
    }

    public void setLatestJobExecEndDate(Date latestJobExecEndDate) {
        this.latestJobExecEndDate = latestJobExecEndDate;
    }

    public List<String> getRunningSteps() {
        return runningSteps;
    }

    public void setRunningSteps(List<String> runningSteps) {
        this.runningSteps = runningSteps;
    }

    public String getBillingPeriodStr() {
        return billingPeriodStr;
    }

    public void setBillingPeriodStr(String billingPeriodStr) {
        this.billingPeriodStr = billingPeriodStr;
    }

    public boolean isStlCalculation() {
        return stlCalculation;
    }

    public void setStlCalculation(boolean stlCalculation) {
        this.stlCalculation = stlCalculation;
    }

    public Date getMaxPartialCalcRunDate() {
        return maxPartialCalcRunDate;
    }

    public void setMaxPartialCalcRunDate(Date maxPartialCalcRunDate) {
        this.maxPartialCalcRunDate = maxPartialCalcRunDate;
    }

    public Date getGmrCalcRunDate() {
        return gmrCalcRunDate;
    }

    public void setGmrCalcRunDate(Date gmrCalcRunDate) {
        this.gmrCalcRunDate = gmrCalcRunDate;
    }

    // helper methods / properties

    // consider gmr/vat recalculation if max partial calculation runDate > gmr calculation runDate
    public boolean isForGmrRecalculation() {
        return (maxPartialCalcRunDate != null && gmrCalcRunDate != null) &&
                maxPartialCalcRunDate.compareTo(gmrCalcRunDate) > 0;
    }

    public List<JobCalculationDto> getSortedJobCalculationDtos() {
        return jobCalculationDtos
                .stream()
                // sort by runEndDate desc
                .sorted(Collections.reverseOrder((dto1, dto2) -> dto1.getRunDate().compareTo(dto2.getRunDate())))
                .collect(Collectors.toList());
    }
}
