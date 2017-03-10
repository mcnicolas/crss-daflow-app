package com.pemc.crss.dataflow.app.dto;

/**
 * Created on 1/12/17.
 */
public class TaskRunDto {

    private String jobName;
    private String parentJob;
    private String meterProcessType;
    private String tradingDate;
    private String startDate;
    private String endDate;
    private String marketInformationType;

    // FOR SETTLEMENT AMS OUTPUT GENERATION
    private String amsInvoiceDate;
    private String amsDueDate;
    private String amsRemarksInv;
    private String amsRemarksMf;

    // BILLING PERIOD INFOS
    private Long billingPeriodId;
    private Long billingPeriod;
    private String supplyMonth;
    private String billingPeriodName;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getParentJob() {
        return parentJob;
    }

    public void setParentJob(String parentJob) {
        this.parentJob = parentJob;
    }

    public String getMeterProcessType() {
        return meterProcessType;
    }

    public void setMeterProcessType(String meterProcessType) {
        this.meterProcessType = meterProcessType;
    }

    public String getTradingDate() {
        return tradingDate;
    }

    public void setTradingDate(String tradingDate) {
        this.tradingDate = tradingDate;
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

    public String getMarketInformationType() {
        return marketInformationType;
    }

    public void setMarketInformationType(String marketInformationType) {
        this.marketInformationType = marketInformationType;
    }

    public String getAmsInvoiceDate() {
        return amsInvoiceDate;
    }

    public void setAmsInvoiceDate(final String amsInvoiceDate) {
        this.amsInvoiceDate = amsInvoiceDate;
    }

    public String getAmsDueDate() {
        return amsDueDate;
    }

    public void setAmsDueDate(final String amsDueDate) {
        this.amsDueDate = amsDueDate;
    }

    public String getAmsRemarksInv() {
        return amsRemarksInv;
    }

    public void setAmsRemarksInv(final String amsRemarksInv) {
        this.amsRemarksInv = amsRemarksInv;
    }

    public String getAmsRemarksMf() {
        return amsRemarksMf;
    }

    public void setAmsRemarksMf(final String amsRemarksMf) {
        this.amsRemarksMf = amsRemarksMf;
    }

    public Long getBillingPeriodId() {
        return billingPeriodId;
    }

    public void setBillingPeriodId(Long billingPeriodId) {
        this.billingPeriodId = billingPeriodId;
    }

    public Long getBillingPeriod() {
        return billingPeriod;
    }

    public void setBillingPeriod(Long billingPeriod) {
        this.billingPeriod = billingPeriod;
    }

    public String getSupplyMonth() {
        return supplyMonth;
    }

    public void setSupplyMonth(String supplyMonth) {
        this.supplyMonth = supplyMonth;
    }

    public String getBillingPeriodName() {
        return billingPeriodName;
    }

    public void setBillingPeriodName(String billingPeriodName) {
        this.billingPeriodName = billingPeriodName;
    }

    @Override
    public String toString() {
        return "TaskRunDto{" +
                "jobName='" + jobName + '\'' +
                ", parentJob='" + parentJob + '\'' +
                ", meterProcessType='" + meterProcessType + '\'' +
                ", tradingDate='" + tradingDate + '\'' +
                ", startDate='" + startDate + '\'' +
                ", endDate='" + endDate + '\'' +
                ", marketInformationType='" + marketInformationType + '\'' +
                ", amsInvoiceDate='" + amsInvoiceDate + '\'' +
                ", amsDueDate='" + amsDueDate + '\'' +
                ", amsRemarksInv='" + amsRemarksInv + '\'' +
                ", amsRemarksMf='" + amsRemarksMf + '\'' +
                ", billingPeriodId=" + billingPeriodId +
                ", billingPeriod=" + billingPeriod +
                ", supplyMonth='" + supplyMonth + '\'' +
                ", billingPeriodName='" + billingPeriodName + '\'' +
                '}';
    }
}
