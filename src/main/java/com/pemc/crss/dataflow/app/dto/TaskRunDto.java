package com.pemc.crss.dataflow.app.dto;

import lombok.Data;

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
}
