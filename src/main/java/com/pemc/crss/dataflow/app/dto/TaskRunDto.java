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
    private String amsRemarks;

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

    public String getAmsRemarks() {
        return amsRemarks;
    }

    public void setAmsRemarks(final String amsRemarks) {
        this.amsRemarks = amsRemarks;
    }

    public String toString() {
        return "jobName: " +  jobName + "\n" +
               "startDate: " + startDate + "\n" +
               "endDate: " + endDate + "\n" +
               "marketInformationType: " + marketInformationType + "\n" +
               "amsInvoiceDate: " + amsInvoiceDate + "\n" +
               "amsDueDate: " + amsDueDate + "\n" +
               "amsRemarks: " + amsRemarks + "\n";
    }
}
