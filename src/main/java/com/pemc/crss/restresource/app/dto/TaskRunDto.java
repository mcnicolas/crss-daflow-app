package com.pemc.crss.restresource.app.dto;

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

    public String toString() {
        return "jobName: " +  jobName + "\n" +
               "startDate: " + startDate + "\n" +
               "endDate: " + endDate + "\n" +
               "marketInformationType: " + marketInformationType + "\n";
    }
}
