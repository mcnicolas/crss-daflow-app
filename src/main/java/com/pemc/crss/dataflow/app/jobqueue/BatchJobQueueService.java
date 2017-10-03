package com.pemc.crss.dataflow.app.jobqueue;

import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

public interface BatchJobQueueService {

    void save(BatchJobQueue batchJobQueue);

    BatchJobQueue get(Long id);

    Page<BatchJobQueue> getAllWithStatus(QueueStatus status, Pageable pageable);
}
