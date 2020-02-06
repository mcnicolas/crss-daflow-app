package com.pemc.crss.dataflow.app.dto;

import com.pemc.crss.dataflow.app.support.StlJobStage;
import lombok.Data;
import org.springframework.batch.core.BatchStatus;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Data
// represents partial generate input workspaces / partial calculations / gmr calculations / finalize jobs associated with a groupId
public class JobCalculationDto {

    public JobCalculationDto() {

    }

    public JobCalculationDto(Date runDate, Date runEndDate, Date startDate, Date endDate, String status,
                             StlJobStage jobStage, BatchStatus jobExecStatus) {
        this.runDate = runDate;
        this.runEndDate = runEndDate;
        this.startDate = startDate;
        this.endDate = endDate;
        this.status = status;
        this.jobStage = jobStage;
        this.jobExecStatus = jobExecStatus;
    }

    private Date runDate;
    private Date runEndDate;
    private Date startDate;
    private Date endDate;
    private String status;
    private StlJobStage jobStage;
    private BatchStatus jobExecStatus;
    private Boolean segragateNss;

    private List<TaskSummaryDto> taskSummaryList = new ArrayList<>();
}
