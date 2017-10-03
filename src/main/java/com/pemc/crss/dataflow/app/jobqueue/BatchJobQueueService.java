package com.pemc.crss.dataflow.app.jobqueue;

import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

public interface BatchJobQueueService {

    void save(BatchJobQueue batchJobQueue);

    BatchJobQueue get(Long id);

    Page<BatchJobQueue> getAll(Pageable pageable);
}
