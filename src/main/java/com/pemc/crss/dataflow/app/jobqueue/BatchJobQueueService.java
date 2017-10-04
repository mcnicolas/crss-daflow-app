package com.pemc.crss.dataflow.app.jobqueue;

import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.shared.commons.util.DateTimeUtil;
import com.pemc.crss.shared.commons.util.ModelMapper;
import com.pemc.crss.shared.commons.util.reference.Module;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

public interface BatchJobQueueService {

    void save(BatchJobQueue batchJobQueue);

    BatchJobQueue get(Long id);

    Page<BatchJobQueue> getAllWithStatus(QueueStatus status, Pageable pageable);

    static BatchJobQueue newInst(final Module module, final JobProcess jobProcess, final TaskRunDto taskRunDto) {
        final BatchJobQueue jobQueue = new BatchJobQueue();

        Long runId = taskRunDto.getRunId();

        jobQueue.setUsername(taskRunDto.getCurrentUser());
        jobQueue.setRunId(runId);
        jobQueue.setQueueDate(DateTimeUtil.parseDateTime(runId));
        jobQueue.setModule(module);
        jobQueue.setJobName(taskRunDto.getJobName());
        jobQueue.setJobProcess(jobProcess);
        jobQueue.setStatus(QueueStatus.ON_QUEUE);
        jobQueue.setTaskObj(ModelMapper.toJson(taskRunDto));

        return jobQueue;
    }
}
