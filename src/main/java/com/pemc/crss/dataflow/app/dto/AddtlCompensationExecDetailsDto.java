package com.pemc.crss.dataflow.app.dto;

import java.math.BigDecimal;

public class AddtlCompensationExecDetailsDto {
    private Long runId;
    private String billingId;
    private String mtn;
    private BigDecimal approvedRate;
    private String status;

    public Long getRunId() {
        return runId;
    }

    public void setRunId(Long runId) {
        this.runId = runId;
    }

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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
