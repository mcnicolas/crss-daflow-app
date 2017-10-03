package com.pemc.crss.dataflow.app.dto;

import com.pemc.crss.shared.commons.util.reference.Module;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BatchJobQueueForm {

    private LocalDateTime queueDate;
    private Long runId;
    private Module module;
    private JobProcess jobProcess;
    private QueueStatus status;
    private String jobName;
    private String taskObj;
    private String details;
    private String user;

    public BatchJobQueue toBatchJobQueue() {
        BatchJobQueue batchJobQueue = new BatchJobQueue();
        batchJobQueue.setRunId(runId);
        batchJobQueue.setQueueDate(queueDate);
        batchJobQueue.setModule(module);
        batchJobQueue.setJobProcess(jobProcess);
        batchJobQueue.setStatus(status);
        batchJobQueue.setJobName(jobName);
        batchJobQueue.setTaskObj(taskObj);
        batchJobQueue.setDetails(details);
        batchJobQueue.setUser(user);
        return batchJobQueue;
    }

}
