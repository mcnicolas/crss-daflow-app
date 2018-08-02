package com.pemc.crss.dataflow.app.dto;

import lombok.Data;

@Data
public class MruInstallmentParam {

    private Integer installmentNum;
    private String invoiceDate; // Save in YYY-MM-DD format
    private String dueDate; // Save in YYY-MM-DD format
    private String remarks;
}
