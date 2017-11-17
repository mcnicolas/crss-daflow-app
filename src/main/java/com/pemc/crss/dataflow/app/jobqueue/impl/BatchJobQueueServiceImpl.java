package com.pemc.crss.dataflow.app.jobqueue.impl;

import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.exception.JobAlreadyOnQueueException;
import com.pemc.crss.dataflow.app.jobqueue.BatchJobQueueService;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.ModelMapper;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.entity.QBatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobQueueRepository;
import com.querydsl.core.BooleanBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

@Service
public class BatchJobQueueServiceImpl implements BatchJobQueueService {

    @Autowired
    private BatchJobQueueRepository queueRepository;

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
    public List<BatchJobQueue> findQueuedAndInProgressJobs(final JobProcess jobProcess) {
        return queueRepository.findByJobProcessAndStatusIn(jobProcess, IN_PROGRESS_STATUSES);
    }

    @Override
    public void validateAdjustedProcess(final TaskRunDto taskRunDtoToQueue, JobProcess finalizeJobProcess) {
        List<BatchJobQueue> inProgressJobs = findQueuedAndInProgressJobs(finalizeJobProcess);

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

    private void validate(BatchJobQueue batchJobQueue) {
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
