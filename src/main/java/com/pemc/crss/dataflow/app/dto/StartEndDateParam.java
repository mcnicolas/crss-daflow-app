package com.pemc.crss.dataflow.app.dto;

import lombok.Data;

@Data
public class StartEndDateParam {

    private String startDate; // Save in YYYY-MM-DD format
    private String endDate; // Save in YYYY-MM-DD format
}
