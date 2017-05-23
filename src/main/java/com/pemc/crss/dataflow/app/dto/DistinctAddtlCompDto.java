package com.pemc.crss.dataflow.app.dto;

import org.springframework.batch.core.BatchStatus;

public class DistinctAddtlCompDto {
    private String startDate;
    private String endDate;
    private String pricingCondition;
    private Long jobId;
    private Long groupId;
    private BatchStatus taggingStatus;
    private BatchStatus genFileStatus;
    private String genFileFolderName;
    private String genFileEndTime;

    public DistinctAddtlCompDto(String startDate, String endDate, String pricingCondition) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.pricingCondition = pricingCondition;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
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

    public Long getGroupId() {
        return groupId;
    }

    public void setGroupId(Long groupId) {
        this.groupId = groupId;
    }

    public BatchStatus getTaggingStatus() {
        return taggingStatus;
    }

    public void setTaggingStatus(BatchStatus taggingStatus) {
        this.taggingStatus = taggingStatus;
    }

    public static DistinctAddtlCompDto create(final String startDate, final String endDate, final String pricingCondition) {
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
}
