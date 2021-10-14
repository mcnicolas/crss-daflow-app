package com.pemc.crss.dataflow.app.dto;

import java.math.BigDecimal;
import java.util.List;

public class AddtlCompensationRunDto {

    private String billingId;
    private String mtn;
    private BigDecimal approvedRate;
    private String billingStartDate;
    private String billingEndDate;
    private String pricingCondition;
    private String currentUser;
    private List<StartEndDateParam> startEndDateParams;

    public String getBillingId() {
        return billingId;
    }

    public void setBillingId(String billingId) {
        this.billingId = billingId;
    }

    public String getMtn() {
        return mtn;
    }

    public void setMtn(String mtn) {
        this.mtn = mtn;
    }

    public BigDecimal getApprovedRate() {
        return approvedRate;
    }

    public void setApprovedRate(BigDecimal approvedRate) {
        this.approvedRate = approvedRate;
    }

    public String getBillingStartDate() {
        return billingStartDate;
    }

    public void setBillingStartDate(String billingStartDate) {
        this.billingStartDate = billingStartDate;
    }

    public String getBillingEndDate() {
        return billingEndDate;
    }

    public void setBillingEndDate(String billingEndDate) {
        this.billingEndDate = billingEndDate;
    }

    public String getPricingCondition() {
        return pricingCondition;
    }

    public void setPricingCondition(String pricingCondition) {
        this.pricingCondition = pricingCondition;
    }

    public String getCurrentUser() {
        return currentUser;
    }

    public void setCurrentUser(String currentUser) {
        this.currentUser = currentUser;
    }

    public List<StartEndDateParam> getStartEndDateParams() {
        return startEndDateParams;
    }

    public void setStartEndDateParams(List<StartEndDateParam> startEndDateParams) {
        this.startEndDateParams = startEndDateParams;
    }
}
