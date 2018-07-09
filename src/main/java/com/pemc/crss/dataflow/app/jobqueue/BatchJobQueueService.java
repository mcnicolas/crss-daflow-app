package com.pemc.crss.dataflow.app.jobqueue;

import com.pemc.crss.dataflow.app.dto.BatchJobQueueDisplay;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateTimeUtil;
import com.pemc.crss.shared.commons.util.ModelMapper;
import com.pemc.crss.shared.commons.util.reference.Module;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public interface BatchJobQueueService {

    void save(BatchJobQueue batchJobQueue);

    void updateStatus(long id, QueueStatus status);

    BatchJobQueue get(Long id);

    Page<BatchJobQueue> getAllWithStatus(QueueStatus status, Pageable pageable);

    List<BatchJobQueue> findQueuedAndInProgressJobs(JobProcess jobProcess);

    void validateAdjustedProcess(final TaskRunDto taskRunDtoToQueue, final JobProcess finalizeJobProcess);

    void validateGenIwsAndCalcQueuedJobs(final TaskRunDto taskRunDtoToQueue, final JobProcess jobProcess);

    void setMtnParam(final BatchJobQueueDisplay queueDisplay);

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

        switch (module) {
            case METERING:
                boolean isDaily = taskRunDto.getMeterProcessType() == null
                        || Objects.equals(MeterProcessType.get(taskRunDto.getMeterProcessType()), MeterProcessType.DAILY);

                MeterProcessType meterProcessType = isDaily ? MeterProcessType.DAILY : MeterProcessType.get(
                        taskRunDto.getMeterProcessType());
                jobQueue.setMeterProcessType(meterProcessType);
                break;
            case SETTLEMENT:
                if (Arrays.asList(JobProcess.CALC_AC, JobProcess.FINALIZE_AC, JobProcess.GEN_FILES_AC).contains(jobProcess)) {
                    jobQueue.setMeterProcessType(MeterProcessType.AC);
                } else {
                    jobQueue.setMeterProcessType(MeterProcessType.get(taskRunDto.getMeterProcessType()));
                }
                break;
            default:
        }

        if (jobQueue.getMeterProcessType() == null) {
            jobQueue.setMeterProcessType(MeterProcessType.DEFAULT);
        }


        return jobQueue;
    }
}
