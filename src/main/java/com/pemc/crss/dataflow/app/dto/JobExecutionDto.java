package com.pemc.crss.dataflow.app.dto;


import com.google.common.base.MoreObjects;
import lombok.Data;
import org.springframework.batch.core.BatchStatus;

import java.util.Date;

@Data
public class JobExecutionDto {

    private Long jobExecutionId;
    private Date startTime;
    private Date endTime;
    private String status;

    public BatchStatus getBatchStatus() {
        return status != null ? BatchStatus.valueOf(status) : null;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("jobExecutionId", getJobExecutionId())
                .add("startTime", getStartTime())
                .add("endTime", getEndTime())
                .add("status", getStatus())
                .toString();
    }
}
