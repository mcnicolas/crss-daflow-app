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
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.Iterator;

@Service
public class BatchJobQueueServiceImpl implements BatchJobQueueService {

    @Autowired
    private BatchJobQueueRepository queueRepository;

    public void save(BatchJobQueue batchJobQueue) {
        validate(batchJobQueue);
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

    private void validate(BatchJobQueue batchJobQueue) {
        BooleanBuilder predicate = new BooleanBuilder();
        predicate.and(QBatchJobQueue.batchJobQueue.status.in(QueueStatus.ON_QUEUE, QueueStatus.STARTED, QueueStatus.STARTING));
        Sort sortByRunId = new Sort(new Sort.Order(Sort.Direction.DESC, "runId"));
        Iterator<BatchJobQueue> jobQueueIterator = queueRepository.findAll(predicate, sortByRunId).iterator();
        if (!jobQueueIterator.hasNext()) {
            return;
        }
        BatchJobQueue previousBatchJobQueue = jobQueueIterator.next();
        if (previousBatchJobQueue.getJobName().equalsIgnoreCase(batchJobQueue.getJobName())
                && previousBatchJobQueue.getTaskObj().equalsIgnoreCase(batchJobQueue.getTaskObj())) {
            throw  new JobAlreadyOnQueueException("Same job is already on queue");
        }
    }
}
