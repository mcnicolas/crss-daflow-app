package com.pemc.crss.dataflow.app.jobqueue.impl;

import com.pemc.crss.dataflow.app.dto.BatchJobQueueDisplay;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.exception.JobAlreadyOnQueueException;
import com.pemc.crss.dataflow.app.jobqueue.BatchJobQueueService;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.ModelMapper;
import com.pemc.crss.shared.commons.util.TaskUtil;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.entity.QBatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobQueueRepository;
import com.pemc.crss.shared.core.dataflow.service.BatchJobAddtlParamsService;
import com.querydsl.core.BooleanBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
public class BatchJobQueueServiceImpl implements BatchJobQueueService {

    @Autowired
    private BatchJobQueueRepository queueRepository;

    @Autowired
    protected JobExplorer jobExplorer;

    @Autowired
    private BatchJobAddtlParamsService batchJobAddtlParamsService;

    private static final List<QueueStatus> IN_PROGRESS_STATUSES = Arrays.asList(QueueStatus.ON_QUEUE,
            QueueStatus.STARTED, QueueStatus.STARTING);

    public void save(BatchJobQueue batchJobQueue) {
        validate(batchJobQueue);
        queueRepository.save(batchJobQueue);
    }

    public void updateStatus(long id, QueueStatus status) {
        BatchJobQueue batchJobQueue = queueRepository.findOne(id);
        if (batchJobQueue == null) {
            throw new RuntimeException("Batch job queue with id: {} cannot be found");
        }
        batchJobQueue.setStatus(status);
        queueRepository.save(batchJobQueue);
    }

    public BatchJobQueue get(Long id) {
        return queueRepository.findOne(id);
    }

    public Page<BatchJobQueue> getAllWithStatus(QueueStatus status, Pageable pageable) {
        if (status == null) {
            return queueRepository.findAll(pageable);
        }
        BooleanBuilder predicate = new BooleanBuilder();
        predicate.and(QBatchJobQueue.batchJobQueue.status.eq(status));
        return queueRepository.findAll(predicate, pageable);
    }

    @Override
    public List<BatchJobQueue> findQueuedAndInProgressJobs(final List<JobProcess> jobProcesses) {
        return queueRepository.findByJobProcessInAndStatusIn(jobProcesses, IN_PROGRESS_STATUSES);
    }

    @Override
    public void validateAdjustedProcess(final TaskRunDto taskRunDtoToQueue, JobProcess finalizeJobProcess) {
        List<BatchJobQueue> inProgressJobs = findQueuedAndInProgressJobs(Collections.singletonList(finalizeJobProcess));

        boolean finalizeJobInProgress = inProgressJobs.stream()
                .map(jobQueue -> ModelMapper.toModel(jobQueue.getTaskObj(), TaskRunDto.class))
                .anyMatch(taskRunDto ->
                    Objects.equals(taskRunDto.getBaseStartDate(), taskRunDtoToQueue.getBaseStartDate()) &&
                    Objects.equals(taskRunDto.getBaseEndDate(), taskRunDtoToQueue.getBaseEndDate()) &&
                    Objects.equals(taskRunDto.getMeterProcessType(), MeterProcessType.ADJUSTED.name()));

        if (finalizeJobInProgress) {
            throw new RuntimeException(String.format("Cannot queue job. A %s ADJUSTED job with the same billing period "
                    + "is already queued.", finalizeJobProcess));
        }
    }

    @Override
    public void validateGenIwsAndCalcQueuedJobs(TaskRunDto taskRunDtoToQueue) {
        List<BatchJobQueue> inProgressJobs = findQueuedAndInProgressJobs(Arrays.asList(JobProcess.GEN_INPUT_WS_TA,
                JobProcess.CALC_TA));

        boolean sameJobInProgress = inProgressJobs.stream()
                .map(jobQueue -> ModelMapper.toModel(jobQueue.getTaskObj(), TaskRunDto.class))
                .anyMatch(taskRunDto ->
                    Objects.equals(taskRunDto.getGroupId(), taskRunDtoToQueue.getGroupId()) &&
                    Objects.equals(taskRunDto.getStartDate(), taskRunDtoToQueue.getStartDate()) &&
                    Objects.equals(taskRunDto.getEndDate(), taskRunDtoToQueue.getEndDate()) &&
                    Objects.equals(taskRunDto.getMeterProcessType(), taskRunDtoToQueue.getMeterProcessType()) &&
                    Objects.equals(taskRunDto.getRegionGroup(), taskRunDtoToQueue.getRegionGroup())
                );

        if (sameJobInProgress) {
            throw new RuntimeException(String.format("Cannot queue job. Another job with the same group_id (%s),"
                    + " trading date (%s), process type (%s) and region_group (%s) is already queued or is currently running.",
                    taskRunDtoToQueue.getGroupId(), taskRunDtoToQueue.getEndDate(),
                    taskRunDtoToQueue.getMeterProcessType(), taskRunDtoToQueue.getRegionGroup()));
        }
    }

    @Override
    public void setMtnParam(final BatchJobQueueDisplay queueDisplay) {
        Long parentJobId = queueDisplay.getMeteringParentId();

        try {
            JobInstance jobInstance = jobExplorer.getJobInstance(parentJobId);
            if (jobInstance != null) {
                JobParameters parameters = jobExplorer.getJobExecutions(jobInstance).get(0).getJobParameters();
                Long runId = parameters.getLong(TaskUtil.RUN_ID);

                String mtns = batchJobAddtlParamsService.getBatchJobAddtlParamsStringVal(runId, "mtns");

                if (StringUtils.isNotEmpty(mtns)) {
                    queueDisplay.getParamMap().put("MTN", mtns);
                } else {
                    queueDisplay.getParamMap().put("MTN", "ALL");
                }

                String regionGroup = parameters.getString("regionGroup");
                String rg = parameters.getString("rg");
                if (StringUtils.isNotEmpty(regionGroup) || StringUtils.isNotEmpty(rg)) {
                    queueDisplay.getParamMap().put("Region Group", StringUtils.isNotEmpty(regionGroup) ? regionGroup : rg);
                } else {
                    queueDisplay.getParamMap().put("Region Group", "ALL");
                }

                /*String region = parameters.getString("region");
                if (StringUtils.isNotEmpty(region)) {
                    queueDisplay.getParamMap().put("Region", region);
                } else {
                    queueDisplay.getParamMap().put("Region", "ALL");
                }*/
            }
        } catch (Exception e) {
            log.warn("Unable to set mtn param due to {}", e.getMessage());
        }
    }

    private void validate(final BatchJobQueue batchJobQueue) {
        BooleanBuilder predicate = new BooleanBuilder();
        predicate.and(QBatchJobQueue.batchJobQueue.status.in(IN_PROGRESS_STATUSES));
        Sort sortByRunId = new Sort(new Sort.Order(Sort.Direction.DESC, "runId"));
        Iterator<BatchJobQueue> jobQueueIterator = queueRepository.findAll(predicate, sortByRunId).iterator();
        if (!jobQueueIterator.hasNext()) {
            return;
        }
        BatchJobQueue previousBatchJobQueue = jobQueueIterator.next();

        TaskRunDto current = ModelMapper.toModel(batchJobQueue.getTaskObj(), TaskRunDto.class);
        TaskRunDto previous = ModelMapper.toModel(previousBatchJobQueue.getTaskObj(), TaskRunDto.class);

        if (current.equals(previous)) {
            throw new JobAlreadyOnQueueException("Job is already on queue!");
        }
    }
}
