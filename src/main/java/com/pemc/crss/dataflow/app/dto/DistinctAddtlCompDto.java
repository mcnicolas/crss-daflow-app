package com.pemc.crss.dataflow.app.dto;

import org.springframework.batch.core.BatchStatus;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DistinctAddtlCompDto {

    private Date startDate;
    private Date endDate;
    private String pricingCondition;
    private Long jobId;
    private String groupId;
    private BatchStatus calcGmrVatStatus;
    private BatchStatus taggingStatus;
    private BatchStatus genFileStatus;
    private String currentStatus;
    private String genFileFolderName;
    private String genFileEndTime;
    private List<TaskSummaryDto> calcGmrVatAcRunSummary = new ArrayList<>();
    private List<TaskSummaryDto> finalizeAcRunSummary = new ArrayList<>();
    private List<String> calcGmrVatRunningSteps = new ArrayList<>();
    private List<String> finalizeRunningSteps = new ArrayList<>();
    private List<String> generateFileRunningSteps = new ArrayList<>();
    private List<Long> successfullAcRuns = new ArrayList<>();
    private boolean locked = false;
    private Long maxAmsRemarksLength;
    private Long amsInvoiceDateRestrictDays;
    private Long amsDueDateRestrictDays;

    public DistinctAddtlCompDto(Date startDate, Date endDate, String pricingCondition, String groupId) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.pricingCondition = pricingCondition;
        this.groupId = groupId;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public String getPricingCondition() {
        return pricingCondition;
    }

    public void setPricingCondition(String pricingCondition) {
        this.pricingCondition = pricingCondition;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public BatchStatus getCalcGmrVatStatus() {
        return calcGmrVatStatus;
    }

    public void setCalcGmrVatStatus(BatchStatus calcGmrVatStatus) {
        this.calcGmrVatStatus = calcGmrVatStatus;
    }

    public BatchStatus getTaggingStatus() {
        return taggingStatus;
    }

    public void setTaggingStatus(BatchStatus taggingStatus) {
        this.taggingStatus = taggingStatus;
    }

    public static DistinctAddtlCompDto create(final Date startDate, final Date endDate, final String pricingCondition,
                                              final String groupId) {
        return new DistinctAddtlCompDto(startDate, endDate, pricingCondition, groupId);
    }

    public BatchStatus getGenFileStatus() {
        return genFileStatus;
    }

    public void setGenFileStatus(BatchStatus genFileStatus) {
        this.genFileStatus = genFileStatus;
    }

    public String getCurrentStatus() {
        return currentStatus;
    }

    public void setCurrentStatus(String currentStatus) {
        this.currentStatus = currentStatus;
    }

    public String getGenFileFolderName() {
        return genFileFolderName;
    }

    public void setGenFileFolderName(String genFileFolderName) {
        this.genFileFolderName = genFileFolderName;
    }

    public String getGenFileEndTime() {
        return genFileEndTime;
    }

    public void setGenFileEndTime(String genFileEndTime) {
        this.genFileEndTime = genFileEndTime;
    }

    public List<TaskSummaryDto> getCalcGmrVatAcRunSummary() {
        return calcGmrVatAcRunSummary;
    }

    public void setCalcGmrVatAcRunSummary(List<TaskSummaryDto> calcGmrVatAcRunSummary) {
        this.calcGmrVatAcRunSummary = calcGmrVatAcRunSummary;
    }

    public List<TaskSummaryDto> getFinalizeAcRunSummary() {
        return finalizeAcRunSummary;
    }

    public void setFinalizeAcRunSummary(List<TaskSummaryDto> finalizeAcRunSummary) {
        this.finalizeAcRunSummary = finalizeAcRunSummary;
    }

    public List<String> getCalcGmrVatRunningSteps() {
        return calcGmrVatRunningSteps;
    }

    public void setCalcGmrVatRunningSteps(List<String> calcGmrVatRunningSteps) {
        this.calcGmrVatRunningSteps = calcGmrVatRunningSteps;
    }

    public List<String> getFinalizeRunningSteps() {
        return finalizeRunningSteps;
    }

    public void setFinalizeRunningSteps(List<String> finalizeRunningSteps) {
        this.finalizeRunningSteps = finalizeRunningSteps;
    }

    public List<String> getGenerateFileRunningSteps() {
        return generateFileRunningSteps;
    }

    public void setGenerateFileRunningSteps(List<String> generateFileRunningSteps) {
        this.generateFileRunningSteps = generateFileRunningSteps;
    }

    public List<Long> getSuccessfullAcRuns() {
        return successfullAcRuns;
    }

    public void setSuccessfullAcRuns(List<Long> successfullAcRuns) {
        this.successfullAcRuns = successfullAcRuns;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public Long getMaxAmsRemarksLength() {
        return maxAmsRemarksLength;
    }

    public void setMaxAmsRemarksLength(Long maxAmsRemarksLength) {
        this.maxAmsRemarksLength = maxAmsRemarksLength;
    }

    public Long getAmsInvoiceDateRestrictDays() {
        return amsInvoiceDateRestrictDays;
    }

    public void setAmsInvoiceDateRestrictDays(Long amsInvoiceDateRestrictDays) {
        this.amsInvoiceDateRestrictDays = amsInvoiceDateRestrictDays;
    }

    public Long getAmsDueDateRestrictDays() {
        return amsDueDateRestrictDays;
    }

    public void setAmsDueDateRestrictDays(Long amsDueDateRestrictDays) {
        this.amsDueDateRestrictDays = amsDueDateRestrictDays;
    }
}
