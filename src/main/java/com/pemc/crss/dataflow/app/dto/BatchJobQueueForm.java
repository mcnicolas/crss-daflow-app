package com.pemc.crss.dataflow.app.dto;

import com.pemc.crss.shared.commons.util.reference.Module;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class BatchJobQueueForm {

    private LocalDateTime queueDate;
    private Module module;
    private JobProcess jobProcess;
    private QueueStatus status;
    private String jobName;
    private String taskObj;
    private String details;
    private Long jobExecId;
    private LocalDateTime jobExecStart;
    private LocalDateTime jobExecEnd;
    private String user;

    public BatchJobQueue toBatchJobQueue() {
        BatchJobQueue batchJobQueue = new BatchJobQueue();
        batchJobQueue.setQueueDate(queueDate);
        batchJobQueue.setModule(module);
        batchJobQueue.setJobProcess(jobProcess);
        batchJobQueue.setStatus(status);
        batchJobQueue.setJobName(jobName);
        batchJobQueue.setTaskObj(taskObj);
        batchJobQueue.setDetails(details);
        batchJobQueue.setJobExecutionId(jobExecId);
        batchJobQueue.setJobExecStart(jobExecStart);
        batchJobQueue.setJobExecEnd(jobExecEnd);
        batchJobQueue.setUser(user);
        return batchJobQueue;
    }

}
