package com.pemc.crss.dataflow.app.dto;

/**
 * Created by kacejo on 5/16/17.
 */
public class AddtlCompensationFinalizeDto {
    private String startDate;
    private String endDate;
    private String pricingCondition;
    private String jobId;
    private String groupId;

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

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }
}
