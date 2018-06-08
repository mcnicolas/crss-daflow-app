package com.pemc.crss.dataflow.app.dto;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Objects;

@Data
public class TaskRunDto {

    private Long runId;
    private String jobName;
    private String parentJob;
    private String groupId;
    private String meterProcessType;
    private String tradingDate;
    private String startDate;
    private String endDate;
    private String baseStartDate;
    private String baseEndDate;
    private String marketInformationType;
    private boolean newGroup;
    private boolean header;
    private String baseType;
    private String currentUser;
    private String meterType;

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

    // yyMMdd (daily) or yyMMddyyMMdd (monthly)
    private String formattedBillingPeriod;

    //MTR, possible values: ALL or specific msp
    private String msp;

    //MTR, possible values: ALL or comma separated sein
    private String seins;

    //Meter process, possible values: ALL or comma separated mtn
    private String mtns;

    //Additional Compensation
    private String billingId;
    private String mtn;
    private BigDecimal approvedRate;
    private String billingStartDate;
    private String billingEndDate;
    private String pricingCondition;
    private String jobId;

    @Override
    public boolean equals(Object o) {
        // do not include runId and ams fields, supplyMonth, billingPeriodName, formattedBillingPeriod, currentUser
        if (this == o) return true;
        if (!(o instanceof TaskRunDto)) return false;

        TaskRunDto that = (TaskRunDto) o;
        return newGroup == that.newGroup &&
                header == that.header &&
                Objects.equals(jobName, that.jobName) &&
                Objects.equals(parentJob, that.parentJob) &&
                Objects.equals(groupId, that.groupId) &&
                Objects.equals(meterProcessType, that.meterProcessType) &&
                Objects.equals(tradingDate, that.tradingDate) &&
                Objects.equals(startDate, that.startDate) &&
                Objects.equals(endDate, that.endDate) &&
                Objects.equals(baseStartDate, that.baseStartDate) &&
                Objects.equals(baseEndDate, that.baseEndDate) &&
                Objects.equals(marketInformationType, that.marketInformationType) &&
                Objects.equals(baseType, that.baseType) &&
                Objects.equals(meterType, that.meterType) &&
                Objects.equals(billingPeriodId, that.billingPeriodId) &&
                Objects.equals(billingPeriod, that.billingPeriod) &&
                Objects.equals(supplyMonth, that.supplyMonth) &&
                Objects.equals(msp, that.msp) &&
                Objects.equals(seins, that.seins) &&
                Objects.equals(mtns, that.mtns) &&
                Objects.equals(billingId, that.billingId) &&
                Objects.equals(mtn, that.mtn) &&
                Objects.equals(approvedRate, that.approvedRate) &&
                Objects.equals(billingStartDate, that.billingEndDate) &&
                Objects.equals(pricingCondition, that.pricingCondition) &&
                Objects.equals(jobId, that.jobId);

    }
}
