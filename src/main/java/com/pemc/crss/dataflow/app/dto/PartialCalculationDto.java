package com.pemc.crss.dataflow.app.dto;

import java.util.Date;

public class PartialCalculationDto {
    private Date runDate;
    private Date runEndDate;
    private Date billingStart;
    private Date billingEnd;
    private String status;

    public Date getRunDate() {
        return runDate;
    }

    public void setRunDate(Date runDate) {
        this.runDate = runDate;
    }

    public Date getRunEndDate() {
        return runEndDate;
    }

    public void setRunEndDate(Date runEndDate) {
        this.runEndDate = runEndDate;
    }

    public Date getBillingStart() {
        return billingStart;
    }

    public void setBillingStart(Date billingStart) {
        this.billingStart = billingStart;
    }

    public Date getBillingEnd() {
        return billingEnd;
    }

    public void setBillingEnd(Date billingEnd) {
        this.billingEnd = billingEnd;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
