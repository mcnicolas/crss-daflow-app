package com.pemc.crss.dataflow.app.dto;

import lombok.Data;
import org.apache.commons.lang3.builder.EqualsBuilder;

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


    @Override
    public boolean equals(Object o) {
        // do not include runId and ams remarks, supplyMonth, billingPeriodName, formattedBillingPeriod, currentUser
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        TaskRunDto that = (TaskRunDto) o;

        return new EqualsBuilder()
                .appendSuper(super.equals(o))
                .append(newGroup, that.newGroup)
                .append(header, that.header)
                .append(jobName, that.jobName)
                .append(parentJob, that.parentJob)
                .append(groupId, that.groupId)
                .append(meterProcessType, that.meterProcessType)
                .append(tradingDate, that.tradingDate)
                .append(startDate, that.startDate)
                .append(endDate, that.endDate)
                .append(baseStartDate, that.baseStartDate)
                .append(baseEndDate, that.baseEndDate)
                .append(marketInformationType, that.marketInformationType)
                .append(baseType, that.baseType)
                .append(meterType, that.meterType)
                .append(billingPeriodId, that.billingPeriodId)
                .append(billingPeriod, that.billingPeriod)
                .append(msp, that.msp)
                .append(seins, that.seins)
                .append(mtns, that.mtns)
                .isEquals();
    }
}
