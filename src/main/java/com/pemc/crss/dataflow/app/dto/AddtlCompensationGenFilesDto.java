package com.pemc.crss.dataflow.app.dto;

import lombok.Data;

@Deprecated
@Data
public class AddtlCompensationGenFilesDto {
    private String startDate;
    private String endDate;
    private String pricingCondition;
    private String groupId;
    private String amsRemarksInv;
    private String amsDueDate;
    private String amsInvoiceDate;
}
