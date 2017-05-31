package com.pemc.crss.dataflow.app.dto;

import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Data
// represents partial calculations / gmr calculations / finalize jobs associated with a groupId
public class JobCalculationDto {

    public JobCalculationDto() {

    }

    public JobCalculationDto(Date runDate, Date runEndDate, Date billingStart, Date billingEnd, String status, String jobStage) {
        this.runDate = runDate;
        this.runEndDate = runEndDate;
        this.billingStart = billingStart;
        this.billingEnd = billingEnd;
        this.status = status;
        this.jobStage = jobStage;
    }

    private Date runDate;
    private Date runEndDate;
    private Date billingStart;
    private Date billingEnd;
    private String status;
    private String jobStage;

    private List<TaskSummaryDto> taskSummaryList = new ArrayList<>();
}
