package com.pemc.crss.dataflow.app.dto;

import com.pemc.crss.shared.commons.util.reference.Module;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
public class BatchJobQueueDisplay {

    private Long id;
    private LocalDateTime queueDate;
    private Module module;
    private JobProcess jobProcess;
    private QueueStatus status;
    private String user;

    public BatchJobQueueDisplay(BatchJobQueue batchJobQueue) {
        this.id = batchJobQueue.getId();
        this.queueDate = batchJobQueue.getQueueDate();
        this.module = batchJobQueue.getModule();
        this.jobProcess = batchJobQueue.getJobProcess();
        this.status = batchJobQueue.getStatus();
        this.user = batchJobQueue.getUsername();
    }

}
