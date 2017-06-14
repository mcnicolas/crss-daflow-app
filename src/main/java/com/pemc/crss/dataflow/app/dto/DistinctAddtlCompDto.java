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
    private BatchStatus taggingStatus;
    private BatchStatus genFileStatus;
    private String genFileFolderName;
    private String genFileEndTime;
    private List<TaskSummaryDto> finalizeAcRunSummary = new ArrayList<>();
    private List<String> finalizeRunningSteps = new ArrayList<>();

    public DistinctAddtlCompDto(Date startDate, Date endDate, String pricingCondition) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.pricingCondition = pricingCondition;
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

    public BatchStatus getTaggingStatus() {
        return taggingStatus;
    }

    public void setTaggingStatus(BatchStatus taggingStatus) {
        this.taggingStatus = taggingStatus;
    }

    public static DistinctAddtlCompDto create(final Date startDate, final Date endDate, final String pricingCondition) {
        return new DistinctAddtlCompDto(startDate, endDate, pricingCondition);
    }

    public BatchStatus getGenFileStatus() {
        return genFileStatus;
    }

    public void setGenFileStatus(BatchStatus genFileStatus) {
        this.genFileStatus = genFileStatus;
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

    public List<TaskSummaryDto> getFinalizeAcRunSummary() {
        return finalizeAcRunSummary;
    }

    public void setFinalizeAcRunSummary(List<TaskSummaryDto> finalizeAcRunSummary) {
        this.finalizeAcRunSummary = finalizeAcRunSummary;
    }

    public List<String> getFinalizeRunningSteps() {
        return finalizeRunningSteps;
    }

    public void setFinalizeRunningSteps(List<String> finalizeRunningSteps) {
        this.finalizeRunningSteps = finalizeRunningSteps;
    }
}
