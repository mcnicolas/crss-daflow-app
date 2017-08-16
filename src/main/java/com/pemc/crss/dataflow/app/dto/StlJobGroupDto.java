package com.pemc.crss.dataflow.app.dto;

import com.pemc.crss.shared.commons.util.DateUtil;
import org.springframework.batch.core.BatchStatus;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

    @Deprecated
    private boolean currentlyRunning;
    @Deprecated
    private boolean latestAdjustment;

    private boolean header;
    private String groupId;
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

    // determines if job with group id and parent id is locked (applied only to FINAL / ADJUSTED)
    private boolean locked = false;

    private List<JobCalculationDto> jobCalculationDtos = new ArrayList<>();

    private Map<String, SortedSet<LocalDate>> remainingDatesMapCalc = new HashMap<>();

    private Map<String, SortedSet<LocalDate>> remainingDatesMapGenIw = new HashMap<>();

    private boolean runningStlCalculation;

    private boolean runningGenInputWorkspace;

    private Date maxPartialCalcRunDate;

    private Date maxPartialGenIwRunDate;

    private Date gmrCalcRunDate;

    private boolean hasCompletedGenInputWs;

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

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
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

    public Map<String, SortedSet<LocalDate>> getRemainingDatesMapCalc() {
        return remainingDatesMapCalc;
    }

    public void setRemainingDatesMapCalc(Map<String, SortedSet<LocalDate>> remainingDatesMapCalc) {
        this.remainingDatesMapCalc = remainingDatesMapCalc;
    }

    public Map<String, SortedSet<LocalDate>> getRemainingDatesMapGenIw() {
        return remainingDatesMapGenIw;
    }

    public void setRemainingDatesMapGenIw(Map<String, SortedSet<LocalDate>> remainingDatesMapGenIw) {
        this.remainingDatesMapGenIw = remainingDatesMapGenIw;
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

    public boolean isRunningStlCalculation() {
        return runningStlCalculation;
    }

    public void setRunningStlCalculation(boolean runningStlCalculation) {
        this.runningStlCalculation = runningStlCalculation;
    }

    public boolean isRunningGenInputWorkspace() {
        return runningGenInputWorkspace;
    }

    public void setRunningGenInputWorkspace(boolean runningGenInputWorkspace) {
        this.runningGenInputWorkspace = runningGenInputWorkspace;
    }

    public Date getMaxPartialCalcRunDate() {
        return maxPartialCalcRunDate;
    }

    public void setMaxPartialCalcRunDate(Date maxPartialCalcRunDate) {
        this.maxPartialCalcRunDate = maxPartialCalcRunDate;
    }

    public Date getMaxPartialGenIwRunDate() {
        return maxPartialGenIwRunDate;
    }

    public void setMaxPartialGenIwRunDate(Date maxPartialGenIwRunDate) {
        this.maxPartialGenIwRunDate = maxPartialGenIwRunDate;
    }

    public Date getGmrCalcRunDate() {
        return gmrCalcRunDate;
    }

    public void setGmrCalcRunDate(Date gmrCalcRunDate) {
        this.gmrCalcRunDate = gmrCalcRunDate;
    }

    public boolean isHasCompletedGenInputWs() {
        return hasCompletedGenInputWs;
    }

    public void setHasCompletedGenInputWs(boolean hasCompletedGenInputWs) {
        this.hasCompletedGenInputWs = hasCompletedGenInputWs;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
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
                // sort by runDate desc
                .sorted(Collections.reverseOrder(Comparator.comparing(JobCalculationDto::getRunDate)))
                .collect(Collectors.toList());
    }
}
