package com.pemc.crss.dataflow.app.jobqueue.impl;

import com.pemc.crss.dataflow.app.exception.JobAlreadyOnQueueException;
import com.pemc.crss.dataflow.app.jobqueue.BatchJobQueueService;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.entity.QBatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobQueueRepository;
import com.querydsl.core.BooleanBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class BatchJobQueueServiceImpl implements BatchJobQueueService {

    @Autowired
    private BatchJobQueueRepository queueRepository;

    @Autowired
    public void save(BatchJobQueue batchJobQueue) {
        validate(batchJobQueue);
        queueRepository.save(batchJobQueue);
    }

    @Autowired
    public BatchJobQueue get(Long id) {
        return queueRepository.findOne(id);
    }

    @Autowired
    public Page<BatchJobQueue> getAllWithStatus(QueueStatus status, Pageable pageable) {
        BooleanBuilder predicate = new BooleanBuilder();
        predicate.and(QBatchJobQueue.batchJobQueue.status.eq(status));
        return queueRepository.findAll(predicate, pageable);
    }

    private void validate(BatchJobQueue batchJobQueue) {
        BooleanBuilder predicate = new BooleanBuilder();
        predicate.and(QBatchJobQueue.batchJobQueue.status.eq(QueueStatus.ON_QUEUE))
                .and(QBatchJobQueue.batchJobQueue.jobName.eq(batchJobQueue.getJobName()))
                .and(QBatchJobQueue.batchJobQueue.taskObj.eq(batchJobQueue.getTaskObj()));
        if (queueRepository.findAll(predicate).iterator().hasNext()) {
            throw  new JobAlreadyOnQueueException("Job is already on queue");
        }
    }
}
