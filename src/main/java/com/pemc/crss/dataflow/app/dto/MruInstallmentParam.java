package com.pemc.crss.dataflow.app.dto;

import lombok.Data;

@Data
public class MruInstallmentParam {

    private Integer installmentNum;
    private String invoiceDate;
    private String dueDate;
    private String remarks;
}
